# Copyright 2022-2024 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

import json
import multiprocessing as mp
import operator
import os
import pickle
from shutil import copytree, rmtree
from typing import Any
from unittest.mock import patch

import pytest
from torch.utils.data import DataLoader

from streaming import MDSWriter, Stream, StreamingDataset
from streaming.base.dataset import _ShardState
from tests.common.utils import convert_to_mds


def validate(remote: str, local: str, dataset: StreamingDataset, keep_zip: bool,
             is_shard_evicted: bool):
    """Validate the number of files in a local directory in comparison to remote directory."""
    if is_shard_evicted:
        ops = operator.lt
    else:
        ops = operator.eq
    if keep_zip:
        if dataset.shards[0].compression:
            # Local has raw + zip, remote has zip.
            assert ops(
                set(filter(lambda f: f == 'index.json' or f.endswith('.zstd'), os.listdir(local))),
                set(os.listdir(remote)))
        else:
            # Local has raw + zip, remote has raw.
            assert ops(set(filter(lambda f: not f.endswith('.zstd'), os.listdir(local))),  \
                set(os.listdir(remote)))
    else:
        if dataset.shards[0].compression:
            # Local has raw, remote has zip.
            assert ops(set(os.listdir(local)),
                       {f.replace('.zstd', '') for f in os.listdir(remote)})
        else:
            # Local has raw, remote has raw.
            assert ops(set(os.listdir(local)), set(os.listdir(remote)))


def shard_eviction_disabled(remote: str, local: str, keep_zip: bool):
    """
    With shard eviction disabled.
    """
    dataset = StreamingDataset(remote=remote, local=local, keep_zip=keep_zip, batch_size=1)
    for _ in range(2):
        for sample in dataset:  # pyright: ignore
            pass

    validate(remote, local, dataset, keep_zip, False)
    rmtree(local, ignore_errors=False)


def shard_eviction_too_high(remote: str, local: str, keep_zip: bool):
    """
    With no shard evictions because cache_limit is bigger than the dataset.
    """
    dataset = StreamingDataset(remote=remote,
                               local=local,
                               keep_zip=keep_zip,
                               cache_limit=1_000_000,
                               batch_size=1)
    dataloader = DataLoader(dataset=dataset, num_workers=8)
    for _ in range(2):
        for _ in dataloader:
            pass
    validate(remote, local, dataset, keep_zip, False)
    rmtree(local, ignore_errors=False)


def shard_eviction(remote: str, local: str, keep_zip: bool):
    """
    With shard eviction because cache_limit is smaller than the whole dataset.
    """
    cache_limit = '120kb' if keep_zip else '100kb'
    dataset = StreamingDataset(remote=remote,
                               local=local,
                               keep_zip=keep_zip,
                               cache_limit=cache_limit,
                               batch_size=1)
    dataloader = DataLoader(dataset=dataset, num_workers=8)
    for _ in range(2):
        for _ in dataloader:
            pass
    validate(remote, local, dataset, keep_zip, True)
    rmtree(local, ignore_errors=False)


def manual_shard_eviction(remote: str, local: str, keep_zip: bool):
    """
    Manually downloading and evicting shards.
    """
    dataset = StreamingDataset(remote=remote, local=local, keep_zip=keep_zip, batch_size=1)

    for shard_id in range(dataset.num_shards):
        dataset.prepare_shard(shard_id)

    full = set(os.listdir(local))

    for shard_id in range(dataset.num_shards):
        dataset.evict_shard(shard_id)

    assert os.listdir(local) == ['index.json']

    for sample_id in range(dataset.num_samples):
        dataset[sample_id]

    assert set(os.listdir(local)) == full
    rmtree(local, ignore_errors=False)


def cache_limit_too_low(remote: str, local: str, keep_zip: bool):
    """
    With impossible shard eviction settings because cache_limit is set too low.
    """
    with pytest.raises(ValueError):
        dataset = StreamingDataset(remote=remote, local=local, cache_limit='1kb', batch_size=1)
        for _ in dataset:
            pass
    rmtree(local, ignore_errors=False)


funcs = [
    shard_eviction_disabled, shard_eviction_too_high, shard_eviction, manual_shard_eviction,
    cache_limit_too_low
]


@pytest.mark.usefixtures('local_remote_dir')
@pytest.mark.parametrize('func', list(funcs))
def test_eviction_nozip(local_remote_dir: tuple[str, str], func: Any):
    num_samples = 5_000
    local, remote = local_remote_dir
    columns = {'data': 'bytes'}
    compression = None
    hashes = None
    size_limit = 500

    with MDSWriter(out=remote,
                   columns=columns,
                   compression=compression,
                   hashes=hashes,
                   size_limit=size_limit) as out:
        for _ in range(num_samples):
            sample = {'data': b'\0'}
            out.write(sample)

    func(remote, local, False)


@pytest.mark.usefixtures('local_remote_dir')
@pytest.mark.parametrize('func', list(funcs))
def test_eviction_zip_nokeep(local_remote_dir: tuple[str, str], func: Any):
    num_samples = 5_000
    local, remote = local_remote_dir
    columns = {'data': 'bytes'}
    compression = 'zstd'
    hashes = None
    size_limit = 500

    with MDSWriter(out=remote,
                   columns=columns,
                   compression=compression,
                   hashes=hashes,
                   size_limit=size_limit) as out:
        for _ in range(num_samples):
            sample = {'data': b'\0'}
            out.write(sample)

    func(remote, local, False)


@pytest.mark.usefixtures('local_remote_dir')
@pytest.mark.parametrize('func', list(funcs))
def test_eviction_zip_keep(local_remote_dir: tuple[str, str], func: Any):
    num_samples = 5_000
    local, remote = local_remote_dir
    columns = {'data': 'bytes'}
    compression = 'zstd'
    hashes = None
    size_limit = 500

    with MDSWriter(out=remote,
                   columns=columns,
                   compression=compression,
                   hashes=hashes,
                   size_limit=size_limit) as out:
        for _ in range(num_samples):
            sample = {'data': b'\0'}
            out.write(sample)

    func(remote, local, True)


@pytest.mark.parametrize('cache_limit', ['2048'])
@pytest.mark.usefixtures('local_remote_dir')
def test_cache_limit_lower_than_index_json(local_remote_dir: Any, cache_limit: str):
    remote_dir, local_dir = local_remote_dir
    convert_to_mds(out_root=remote_dir,
                   dataset_name='sequencedataset',
                   num_samples=1000,
                   compression=None,
                   size_limit=2048)

    with pytest.raises(ValueError, match='Minimum cache usage.*is larger than the cache limit*'):
        _ = StreamingDataset(local=local_dir,
                             remote=remote_dir,
                             shuffle=False,
                             batch_size=4,
                             cache_limit=cache_limit)


@pytest.mark.parametrize('cache_limit', ['5kb'])
@pytest.mark.parametrize('compression', [None, 'zstd:7'])
@pytest.mark.usefixtures('local_remote_dir')
def test_cache_limit_lower_than_few_shards(local_remote_dir: Any, cache_limit: str,
                                           compression: str):
    remote_dir, local_dir = local_remote_dir
    convert_to_mds(out_root=remote_dir,
                   dataset_name='sequencedataset',
                   num_samples=1000,
                   compression=compression,
                   size_limit=2048)

    with pytest.raises(ValueError,
                       match='Cache limit.*is too low. Increase the `cache_limit` to*'):
        _ = StreamingDataset(local=local_dir,
                             remote=remote_dir,
                             shuffle=False,
                             batch_size=4,
                             cache_limit=cache_limit)


@pytest.mark.usefixtures('local_remote_dir')
def test_cache_filelock_reuse(local_remote_dir: tuple[str, str]):
    """Test that _cache_filelock is reused across multiple calls instead of being recreated.

    This test verifies the fix for issue #963 where CACHE_FILELOCK constant had a mismatch
    with the actual attribute name, causing a new FileLock to be created on every call.
    """
    num_samples = 1000
    local, remote = local_remote_dir
    columns = {'data': 'bytes'}
    compression = None
    hashes = None
    size_limit = 500

    # Create a small dataset
    with MDSWriter(out=remote,
                   columns=columns,
                   compression=compression,
                   hashes=hashes,
                   size_limit=size_limit) as out:
        for _ in range(num_samples):
            sample = {'data': b'\0'}
            out.write(sample)

    dataset = StreamingDataset(remote=remote, local=local, batch_size=1)

    # First call to prepare_shard should create the filelock
    assert dataset._cache_filelock is None
    dataset.prepare_shard(0)
    assert dataset._cache_filelock is not None, 'Expected _cache_filelock to be created'

    # Store reference to the filelock object
    first_filelock = dataset._cache_filelock

    # Second call to prepare_shard should reuse the same filelock
    if dataset.num_shards > 1:
        dataset.prepare_shard(1)
        second_filelock = dataset._cache_filelock
        assert first_filelock is second_filelock, \
            'Expected _cache_filelock to be reused, not recreated'

    # Call evict_coldest_shard and verify filelock is still the same
    if dataset.num_shards > 0:
        dataset.evict_coldest_shard()
        third_filelock = dataset._cache_filelock
        assert first_filelock is third_filelock, \
            'Expected _cache_filelock to be reused in evict_coldest_shard'

    # Call evict_shard and verify filelock is still the same
    if dataset.num_shards > 1:
        dataset.prepare_shard(1)  # Prepare shard 1 again
        dataset.evict_shard(1)
        fourth_filelock = dataset._cache_filelock
        assert first_filelock is fourth_filelock, \
            'Expected _cache_filelock to be reused in evict_shard'

    rmtree(local, ignore_errors=False)


def _prepare_and_evict(dataset: StreamingDataset, shard_id: int) -> None:
    dataset.prepare_shard(shard_id)
    dataset.evict_shard(shard_id)


@pytest.mark.skipif('fork' not in mp.get_all_start_methods(), reason='Requires fork.')
@pytest.mark.usefixtures('local_remote_dir')
def test_cache_filelock_used_before_fork(local_remote_dir: tuple[str, str]):
    """A forked child must not fail on a cache filelock the parent created and used."""
    local, remote = local_remote_dir
    convert_to_mds(out_root=remote,
                   dataset_name='sequencedataset',
                   num_samples=117,
                   size_limit=1 << 8)
    dataset = StreamingDataset(remote=remote, local=local, batch_size=1)
    assert dataset.num_shards > 1

    # Create and use the cache filelock in the parent.
    dataset.prepare_shard(0)
    assert dataset._cache_filelock is not None

    # The dataset must still pickle for spawn and forkserver workers.
    pickle.dumps(dataset)

    # A forked child inherits the parent's lock object, and must replace it with its own.
    p = mp.get_context('fork').Process(target=_prepare_and_evict, args=(dataset, 1))
    p.start()
    p.join(60)
    p.terminate()
    assert p.exitcode == 0


def test_evict_coldest_skips_local_only_stream(tmp_path: Any):
    """Do not evict shards whose stream has no remote copy."""
    remote = tmp_path / 'remote'
    local_remote = tmp_path / 'local_remote'
    canonical = tmp_path / 'canonical'
    local_only = tmp_path / 'local_only'
    columns = {'data': 'bytes'}

    for dest in (remote, canonical):
        with MDSWriter(out=str(dest), columns=columns, size_limit=500) as out:
            for _ in range(200):
                out.write({'data': b'\0' * 100})

    copytree(canonical, local_only)
    before = set(os.listdir(local_only))

    dataset = StreamingDataset(streams=[
        Stream(local=str(local_only), remote=None),
        Stream(local=str(local_remote), remote=str(remote)),
    ],
                               batch_size=1,
                               cache_limit=10_000_000)

    for shard_id in range(dataset.num_shards):
        dataset.prepare_shard(shard_id)

    dataset.evict_coldest_shard()
    assert set(os.listdir(local_only)) == before


def test_cache_limit_leaves_room_above_local_only_streams(tmp_path: Any):
    """A cache limit that local-only shards would fill fails at init, not in a download thread."""
    remote = tmp_path / 'remote'
    local_only = tmp_path / 'local_only'
    with MDSWriter(out=str(remote), columns={'data': 'bytes'}, size_limit=500) as out:
        for _ in range(200):
            out.write({'data': b'\0' * 100})
    copytree(remote, local_only)
    shards = json.load(open(remote / 'index.json'))['shards']
    largest = max(shard['raw_data']['bytes'] for shard in shards)
    # Both index files and the local-only shards stay in the cache for good.
    floor = 2 * os.path.getsize(remote / 'index.json') + sum(
        shard['raw_data']['bytes'] for shard in shards)

    def streams() -> list[Stream]:
        return [
            Stream(local=str(local_only), remote=None),
            Stream(local=str(tmp_path / 'cache'), remote=str(remote)),
        ]

    with pytest.raises(ValueError, match='never evicted'):
        StreamingDataset(streams=streams(), batch_size=1, cache_limit=floor + 4 * largest - 1)

    # At the bound there is room for four shards: the one being read, the one the ready thread
    # waits for, and what the download thread fetches ahead, so keep the look-ahead to one shard.
    dataset = StreamingDataset(streams=streams(),
                               batch_size=1,
                               predownload=4,
                               cache_limit=floor + 4 * largest)
    assert sum(1 for _ in dataset) == 400
    assert set(os.listdir(local_only)) == set(os.listdir(remote))


def _write_small_shards(remote: str, compression: Any = None, hashes: Any = None) -> None:
    with MDSWriter(out=remote,
                   columns={'data': 'bytes'},
                   compression=compression,
                   hashes=hashes,
                   size_limit=500) as out:
        for _ in range(200):
            out.write({'data': b'\0'})


def test_failed_download_returns_shard_to_remote(local_remote_dir: tuple[str, str]):
    """A failed download puts the shard back to REMOTE, so the next call downloads it again."""
    local, remote = local_remote_dir
    _write_small_shards(remote)
    dataset = StreamingDataset(remote=remote, local=local, batch_size=1)
    download = Stream.prepare_shard
    calls = []

    def fail_first(self: Stream, shard: Any) -> int:
        calls.append(shard)
        if len(calls) == 1:
            raise OSError('simulated download failure')
        return download(self, shard)

    with patch.object(Stream, 'prepare_shard', fail_first):
        with pytest.raises(OSError, match='simulated download failure'):
            dataset.prepare_shard(0)
        assert dataset._shard_states[0] == _ShardState.REMOTE
        dataset.prepare_shard(0, blocking=False)
    assert dataset._shard_states[0] == _ShardState.LOCAL
    assert len(calls) == 2


def test_failed_decompression_keeps_shard_local(local_remote_dir: tuple[str, str]):
    """A failed decompression restores LOCAL, so the next call decompresses again."""
    local, remote = local_remote_dir
    _write_small_shards(remote, compression='zstd')
    dataset = StreamingDataset(remote=remote, local=local, batch_size=1, keep_zip=True)
    dataset.prepare_shard(0)
    stream = dataset.streams[dataset.stream_per_shard[0]]
    raw_info, _ = dataset.shards[0].file_pairs[0]
    raw_filename = os.path.join(stream.local, stream.split, raw_info.basename)
    os.remove(raw_filename)

    with patch.object(Stream,
                      'prepare_shard',
                      side_effect=OSError('simulated decompression failure')):
        with pytest.raises(OSError, match='simulated decompression failure'):
            dataset.prepare_shard(0)
    assert dataset._shard_states[0] == _ShardState.LOCAL
    dataset.prepare_shard(0, blocking=False)
    assert os.path.isfile(raw_filename)


@pytest.mark.parametrize('compression', [None, 'zstd'])
def test_checksum_failure_downloads_shard_again(local_remote_dir: tuple[str, str],
                                                compression: Any):
    """A downloaded file that fails its checksum is deleted, so the retry downloads it again."""
    local, remote = local_remote_dir
    _write_small_shards(remote, compression, hashes=['sha1'])
    dataset = StreamingDataset(remote=remote, local=local, batch_size=1, validate_hash='sha1')
    raw_info, zip_info = dataset.shards[0].file_pairs[0]
    basename = (zip_info or raw_info).basename
    download = Stream._download_file
    corrupted = []

    def corrupt_first(self: Stream, from_basename: str, to_basename: Any = None) -> str:
        filename = download(self, from_basename, to_basename)
        if from_basename == basename and not corrupted:
            corrupted.append(filename)
            with open(filename, 'r+b') as f:
                f.seek(-1, os.SEEK_END)
                last = f.read(1)[0]
                f.seek(-1, os.SEEK_END)
                f.write(bytes([last ^ 0xff]))
        return filename

    with patch.object(Stream, '_download_file', corrupt_first):
        with pytest.raises(ValueError, match='Checksum failure'):
            dataset.prepare_shard(0)
        assert not os.path.exists(corrupted[0])
        dataset.prepare_shard(0)
    assert dataset._shard_states[0] == _ShardState.LOCAL
    assert dataset[0]['data'] == b'\0'
