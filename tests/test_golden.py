# Copyright 2024 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

"""Golden tests against fixtures written by the last upstream release.

``tests/golden/generate.py`` wrote the fixtures under ``tests/golden/<version>/`` while running
under the released package. The tests here run against the checkout and pin the on-disk shard
formats, the sample order for a fixed shuffle configuration and the ``state_dict`` contract, so a
change that silently breaks compatibility with existing datasets or checkpoints fails here.
"""

import json
import os
import shutil
from typing import Any, Callable, Iterator

import numpy as np
import pytest
import zstd

from streaming import CSVWriter, JSONWriter, MDSWriter, StreamingDataset
from streaming.base.compression import decompress
from streaming.base.hashing import get_hash
from streaming.base.util import clean_stale_shared_memory
from tests.golden import content

GOLDEN_VERSION = '0.13.0'
GOLDEN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'golden', GOLDEN_VERSION)

STATE_DICT_KEYS = {
    'epoch', 'sample_in_epoch', 'num_canonical_nodes', 'shuffle_seed', 'initial_physical_nodes'
}

WRITERS = {'MDSWriter': MDSWriter, 'JSONWriter': JSONWriter, 'CSVWriter': CSVWriter}
FORMATS = {'MDSWriter': 'mds', 'JSONWriter': 'json', 'CSVWriter': 'csv'}


def _load_json(basename: str) -> Any:
    with open(os.path.join(GOLDEN_DIR, basename)) as f:
        return json.load(f)


MANIFEST = _load_json('manifest.json')
ORDER = _load_json('order.json')
STATE = _load_json('state_dict.json')
DATASETS = sorted(MANIFEST['datasets'])


def _order_id(entry: dict[str, Any]) -> str:
    algo = entry['shuffle_algo'] if entry['shuffle'] else 'noshuffle'
    block = f"-b{entry['shuffle_block_size']}" if entry['shuffle_block_size'] else ''
    return f"{algo}-s{entry['shuffle_seed']}-n{entry['num_canonical_nodes']}{block}"


def _read(dirname: str, basename: str) -> bytes:
    with open(os.path.join(dirname, basename), 'rb') as f:
        return f.read()


def _dataset_kwargs(entry: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in entry.items() if key != 'ids'}


def _without_zip(index: dict[str, Any]) -> dict[str, Any]:
    """Drop the compressed-file entries, which depend on the compressor version."""
    shards = []
    for shard in index['shards']:
        shard = dict(shard)
        shard.pop('zip_data', None)
        shard.pop('zip_meta', None)
        shards.append(shard)
    return {**index, 'shards': shards}


def _assert_sample_equal(expected: dict[str, Any], actual: dict[str, Any],
                         columns: dict[str, str]) -> None:
    assert set(actual) == set(columns)
    for name, encoding in columns.items():
        want = expected[name]
        got = actual[name]
        if encoding.startswith('ndarray'):
            assert isinstance(got, np.ndarray), name
            assert got.dtype == want.dtype, name
            assert got.shape == want.shape, name
            assert np.array_equal(got, want), name
        elif encoding in ('float64', 'uint8'):
            assert isinstance(got, np.generic), name
            assert got.dtype == np.dtype(encoding), name
            assert got == want, name
        elif encoding in ('int', 'str_int'):
            assert type(got) is int, name
            assert got == want, name
        elif encoding == 'float':
            assert type(got) is float, name
            assert got == want, name
        else:
            assert type(got) is type(want), name
            assert got == want, name


@pytest.fixture(autouse=True)
def clean_shared_memory() -> Iterator[None]:
    """Start and finish every test without shared memory left over from another dataset."""
    clean_stale_shared_memory()
    yield
    clean_stale_shared_memory()


@pytest.fixture
def golden_copy(local_remote_dir: tuple[str, str]) -> Callable[[str], str]:
    """Copy a fixture dataset into a private local directory.

    Reading a dataset may write into its local directory (for example decompressing shards), so
    the fixtures are never opened in place.
    """
    local, _ = local_remote_dir

    def copy(name: str) -> str:
        dirname = os.path.join(local, name)
        shutil.copytree(os.path.join(GOLDEN_DIR, name), dirname)
        return dirname

    return copy


def test_manifest():
    assert MANIFEST['streaming'] == GOLDEN_VERSION
    assert MANIFEST['num_samples'] == content.NUM_SAMPLES
    assert set(MANIFEST['datasets']) == {'mds-none', 'mds-zstd', 'json', 'csv'}


@pytest.mark.parametrize('name', DATASETS)
def test_fixture_index_format(name: str):
    """The index is ``{"shards": [...], "version": 2}`` dumped with ``sort_keys=True``."""
    text = _read(os.path.join(GOLDEN_DIR, name), 'index.json').decode('utf-8')
    index = json.loads(text)
    assert set(index) == {'shards', 'version'}
    assert index['version'] == 2
    assert text == json.dumps(index, sort_keys=True)
    assert len(index['shards']) in (2, 3)
    for shard in index['shards']:
        assert shard['version'] == 2
        assert shard['format'] == FORMATS[MANIFEST['datasets'][name]['writer']]
        assert shard['hashes'] == ['sha1']


def _assert_file_info(info: dict[str, Any], data: bytes) -> None:
    assert len(data) == info['bytes'], info['basename']
    assert get_hash('sha1', data) == info['hashes']['sha1'], info['basename']


@pytest.mark.parametrize('name', DATASETS)
def test_fixture_integrity(name: str):
    """Every shard file matches the size and sha1 its index entry records.

    A compressed dataset only ships the compressed file, so its raw entry is checked against the
    decompressed bytes.
    """
    dirname = os.path.join(GOLDEN_DIR, name)
    index = json.loads(_read(dirname, 'index.json'))
    files = {'index.json'}
    for shard in index['shards']:
        for raw_key, zip_key in (('raw_data', 'zip_data'), ('raw_meta', 'zip_meta')):
            raw_info = shard.get(raw_key)
            zip_info = shard.get(zip_key)
            if raw_info is None:
                continue
            if zip_info is None:
                raw = _read(dirname, raw_info['basename'])
                files.add(raw_info['basename'])
            else:
                zipped = _read(dirname, zip_info['basename'])
                files.add(zip_info['basename'])
                _assert_file_info(zip_info, zipped)
                raw = decompress(shard['compression'], zipped)
            _assert_file_info(raw_info, raw)
    assert sorted(os.listdir(dirname)) == sorted(files)


@pytest.mark.parametrize('name', DATASETS)
def test_read_back(golden_copy: Callable[[str], str], name: str):
    """The checkout reads every sample the release wrote, field by field."""
    columns = MANIFEST['datasets'][name]['columns']
    dataset = StreamingDataset(local=golden_copy(name), shuffle=False, batch_size=1)
    samples = content.make_samples()
    assert len(dataset) == len(samples)
    for index, sample in enumerate(samples):
        _assert_sample_equal(content.project(sample, columns), dataset[index], columns)


@pytest.mark.parametrize('name', DATASETS)
def test_write_identity(local_remote_dir: tuple[str, str], name: str):
    """The checkout's writer reproduces the release's files byte for byte.

    Compressed bytes are only expected to match when the installed zstd is the version that wrote
    the fixture; otherwise the decompressed shards and the index minus its ``zip_*`` entries must
    match.
    """
    spec = MANIFEST['datasets'][name]
    local, _ = local_remote_dir
    out = os.path.join(local, name)
    with WRITERS[spec['writer']](out=out,
                                 columns=spec['columns'],
                                 compression=spec['compression'],
                                 hashes=spec['hashes'],
                                 size_limit=spec['size_limit']) as writer:
        for sample in content.make_samples():
            writer.write(content.project(sample, spec['columns']))

    fixture = os.path.join(GOLDEN_DIR, name)
    assert sorted(os.listdir(out)) == sorted(os.listdir(fixture))
    expect_identical = spec['compression'] is None or zstd.version() == MANIFEST['zstd']
    for basename in sorted(os.listdir(fixture)):
        want = _read(fixture, basename)
        got = _read(out, basename)
        if expect_identical:
            assert got == want, f'{name}/{basename} differs from the {GOLDEN_VERSION} fixture'
        elif basename == 'index.json':
            assert _without_zip(json.loads(got)) == _without_zip(json.loads(want))
        else:
            assert decompress(spec['compression'], got) == decompress(spec['compression'], want)


@pytest.mark.parametrize('entry', ORDER['entries'], ids=_order_id)
def test_sample_order(golden_copy: Callable[[str], str], entry: dict[str, Any]):
    """A fixed shuffle configuration yields the same sample order as the release."""
    dataset = StreamingDataset(local=golden_copy(ORDER['dataset']), **_dataset_kwargs(entry))
    assert [sample['id'] for sample in dataset] == entry['ids']


def test_state_dict_mid_epoch(golden_copy: Callable[[str], str]):
    """``state_dict`` has exactly the contract keys and matches the release mid-epoch."""
    num_consumed = STATE['num_consumed']
    dataset = StreamingDataset(local=golden_copy(STATE['dataset']), **STATE['args'])
    it = iter(dataset)
    assert [next(it)['id'] for _ in range(num_consumed)] == STATE['consumed_ids']

    from_beginning = dataset.state_dict(num_samples=num_consumed, from_beginning=True)
    assert set(from_beginning) == STATE_DICT_KEYS
    assert all(type(value) is int for value in from_beginning.values())
    assert from_beginning == STATE['state_dict_from_beginning']
    not_from_beginning = dataset.state_dict(num_samples=num_consumed, from_beginning=False)
    assert not_from_beginning == STATE['state_dict_not_from_beginning']

    assert [sample['id'] for sample in it] == STATE['remaining_ids']


def test_state_dict_round_trip(golden_copy: Callable[[str], str]):
    """Loading a release state_dict reproduces it and resumes at the release's next sample."""
    loaded = STATE['state_dict_from_beginning']
    dataset = StreamingDataset(local=golden_copy(STATE['dataset']), **STATE['args'])
    dataset.load_state_dict(dict(loaded))
    reloaded = dataset.state_dict(num_samples=0, from_beginning=False)
    assert reloaded == loaded
    assert reloaded == STATE['state_dict_after_load']

    resumed = [sample['id'] for sample in dataset]
    assert resumed == STATE['resumed_ids']
    after = dataset.state_dict(num_samples=len(resumed), from_beginning=False)
    assert after == STATE['state_dict_after_resume']

    # Consumed then resumed is exactly one full epoch in the order recorded for this configuration.
    full_epoch = [
        entry['ids'] for entry in ORDER['entries'] if entry['shuffle_block_size'] is None and {
            key: entry[key] for key in STATE['args']
        } == STATE['args']
    ]
    assert full_epoch == [STATE['consumed_ids'] + resumed]


def test_rejects_unknown_index_version(golden_copy: Callable[[str], str]):
    """The reader only accepts index version 2."""
    local = golden_copy('mds-none')
    filename = os.path.join(local, 'index.json')
    with open(filename) as f:
        index = json.load(f)
    index['version'] = 3
    with open(filename, 'w') as f:
        json.dump(index, f, sort_keys=True)
    with pytest.raises(ValueError, match='Unsupported streaming data version: 3'):
        StreamingDataset(local=local, batch_size=1)
