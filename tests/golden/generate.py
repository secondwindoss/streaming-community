# Copyright 2024 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

"""Write the golden fixtures with the installed release of streaming.

Run this under a virtualenv holding the release to pin, not the checkout, for example::

    python tests/golden/generate.py            # writes tests/golden/<streaming.__version__>/

The fixtures are read back by ``tests/test_golden.py`` running against the checkout. Only
regenerate them to pin a new release; never regenerate them to make a failing golden test pass.
"""

import argparse
import gc
import json
import os
import platform
import shutil
import sys
import tempfile
from typing import Any, Optional

import numpy as np
import zstd

import streaming
from streaming import CSVWriter, JSONWriter, MDSWriter, StreamingDataset
from streaming.base.util import clean_stale_shared_memory

# Imported by path so this script needs neither the ``tests`` package nor the checkout on sys.path.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import content  # noqa: E402

HASHES = ['sha1']
MDS_SIZE_LIMIT = 5120
SPLIT_SIZE_LIMIT = 2048
BATCH_SIZE = 4

SHUFFLE_SEEDS = (9176, 42)
NUM_CANONICAL_NODES = (1, 2, 4)
SHUFFLE_ALGOS = ('py1e', 'py1br', 'py1s')

# Where to take the mid-epoch state_dict from, and the dataset arguments it is taken under.
STATE_DICT_NUM_CONSUMED = 12
STATE_DICT_ARGS = {
    'shuffle': True,
    'shuffle_algo': 'py1e',
    'shuffle_seed': 9176,
    'num_canonical_nodes': 2,
    'batch_size': BATCH_SIZE,
}


def write_datasets(out_root: str, samples: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Write one dataset per format under ``out_root``.

    Args:
        out_root (str): Fixture root.
        samples (List[Dict[str, Any]]): Samples from :mod:`content`.

    Returns:
        Dict[str, Dict[str, Any]]: Writer arguments per dataset, for the manifest.
    """
    specs: dict[str, dict[str, Any]] = {
        'mds-none': {
            'writer': 'MDSWriter',
            'columns': content.MDS_COLUMNS,
            'compression': None,
            'size_limit': MDS_SIZE_LIMIT,
        },
        'mds-zstd': {
            'writer': 'MDSWriter',
            'columns': content.MDS_COLUMNS,
            'compression': 'zstd',
            'size_limit': MDS_SIZE_LIMIT,
        },
        'json': {
            'writer': 'JSONWriter',
            'columns': content.JSON_COLUMNS,
            'compression': None,
            'size_limit': SPLIT_SIZE_LIMIT,
        },
        'csv': {
            'writer': 'CSVWriter',
            'columns': content.XSV_COLUMNS,
            'compression': None,
            'size_limit': SPLIT_SIZE_LIMIT,
        },
    }
    writers = {'MDSWriter': MDSWriter, 'JSONWriter': JSONWriter, 'CSVWriter': CSVWriter}
    for name, spec in specs.items():
        spec['hashes'] = HASHES
        writer_cls = writers[spec['writer']]
        with writer_cls(out=os.path.join(out_root, name),
                        columns=spec['columns'],
                        compression=spec['compression'],
                        hashes=spec['hashes'],
                        size_limit=spec['size_limit']) as out:
            for sample in samples:
                out.write(content.project(sample, spec['columns']))
    return specs


def fresh_dataset(fixture_dir: str, scratch: str, **kwargs: Any) -> StreamingDataset:
    """Open a dataset on a private copy of a fixture so nothing is written into the fixture.

    Args:
        fixture_dir (str): The fixture dataset directory.
        scratch (str): Scratch root; each call gets a new subdirectory.
        **kwargs (Any): Passed to :class:`StreamingDataset`.

    Returns:
        StreamingDataset: The dataset.
    """
    local = tempfile.mkdtemp(dir=scratch)
    shutil.rmtree(local)
    shutil.copytree(fixture_dir, local)
    return StreamingDataset(local=local, **kwargs)


def release(dataset: Optional[StreamingDataset]) -> None:
    """Drop a dataset and clean its shared memory before the next one is created.

    Args:
        dataset (StreamingDataset, optional): The dataset to drop.
    """
    del dataset
    gc.collect()
    clean_stale_shared_memory()


def record_orders(fixture_dir: str, scratch: str) -> list[dict[str, Any]]:
    """Record the single-process sample order for each shuffle configuration.

    Args:
        fixture_dir (str): The ``mds-none`` fixture directory.
        scratch (str): Scratch root.

    Returns:
        List[Dict[str, Any]]: Dataset arguments plus the ``id`` order they produce.
    """
    configs: list[dict[str, Any]] = [{
        'shuffle': False,
        'shuffle_algo': 'py1e',
        'shuffle_seed': 9176,
        'num_canonical_nodes': 1,
        'shuffle_block_size': None,
        'batch_size': BATCH_SIZE,
    }]
    for algo in SHUFFLE_ALGOS:
        for seed in SHUFFLE_SEEDS:
            for num_canonical_nodes in NUM_CANONICAL_NODES:
                configs.append({
                    'shuffle': True,
                    'shuffle_algo': algo,
                    'shuffle_seed': seed,
                    'num_canonical_nodes': num_canonical_nodes,
                    'shuffle_block_size': None,
                    'batch_size': BATCH_SIZE,
                })
    # A block size smaller than the dataset exercises the block logic of the block shufflers.
    for algo in ('py1e', 'py1br'):
        configs.append({
            'shuffle': True,
            'shuffle_algo': algo,
            'shuffle_seed': 9176,
            'num_canonical_nodes': 2,
            'shuffle_block_size': 32,
            'batch_size': BATCH_SIZE,
        })

    entries = []
    for config in configs:
        dataset = fresh_dataset(fixture_dir, scratch, **config)
        ids = [int(sample['id']) for sample in dataset]
        release(dataset)
        entry = dict(config)
        entry['ids'] = ids
        entries.append(entry)
    return entries


def record_state_dict(fixture_dir: str, scratch: str) -> dict[str, Any]:
    """Record state_dict output mid-epoch and the order after resuming from it.

    Args:
        fixture_dir (str): The ``mds-none`` fixture directory.
        scratch (str): Scratch root.

    Returns:
        Dict[str, Any]: The recorded state dicts and sample orders.
    """
    dataset = fresh_dataset(fixture_dir, scratch, **STATE_DICT_ARGS)
    it = iter(dataset)
    consumed_ids = [int(next(it)['id']) for _ in range(STATE_DICT_NUM_CONSUMED)]
    from_beginning = dataset.state_dict(num_samples=STATE_DICT_NUM_CONSUMED, from_beginning=True)
    not_from_beginning = dataset.state_dict(num_samples=STATE_DICT_NUM_CONSUMED,
                                            from_beginning=False)
    remaining_ids = [int(sample['id']) for sample in it]
    release(dataset)

    dataset = fresh_dataset(fixture_dir, scratch, **STATE_DICT_ARGS)
    dataset.load_state_dict(from_beginning)
    reloaded = dataset.state_dict(num_samples=0, from_beginning=False)
    resumed_ids = [int(sample['id']) for sample in dataset]
    after_resume = dataset.state_dict(num_samples=len(resumed_ids), from_beginning=False)
    release(dataset)

    return {
        'args': STATE_DICT_ARGS,
        'num_consumed': STATE_DICT_NUM_CONSUMED,
        'consumed_ids': consumed_ids,
        'remaining_ids': remaining_ids,
        'state_dict_from_beginning': from_beginning,
        'state_dict_not_from_beginning': not_from_beginning,
        'state_dict_after_load': reloaded,
        'resumed_ids': resumed_ids,
        'state_dict_after_resume': after_resume,
    }


def format_json(obj: Any, depth: int = 0) -> str:
    """Format JSON with indented objects but lists of scalars on one line.

    Args:
        obj (Any): JSON-serializable object.
        depth (int): Current nesting depth. Defaults to ``0``.

    Returns:
        str: The formatted JSON.
    """
    pad = '    ' * (depth + 1)
    end = '    ' * depth
    if isinstance(obj, dict) and obj:
        items = [
            f'{pad}{json.dumps(key)}: {format_json(value, depth + 1)}'
            for key, value in obj.items()
        ]
        return '{\n' + ',\n'.join(items) + f'\n{end}}}'
    if isinstance(obj, list) and any(isinstance(item, (dict, list)) for item in obj):
        items = [f'{pad}{format_json(item, depth + 1)}' for item in obj]
        return '[\n' + ',\n'.join(items) + f'\n{end}]'
    return json.dumps(obj)


def dump_json(filename: str, obj: Any) -> None:
    """Write JSON formatted by :func:`format_json` with a trailing newline.

    Args:
        filename (str): Output path.
        obj (Any): JSON-serializable object.
    """
    with open(filename, 'w') as out:
        out.write(format_json(obj) + '\n')


def main(args: argparse.Namespace) -> None:
    """Write the fixtures.

    Args:
        args (argparse.Namespace): Command-line arguments.
    """
    if 'dev' in streaming.__version__:
        raise RuntimeError(f'Fixtures must come from a released package, not ' +
                           f'{streaming.__file__} ({streaming.__version__}). Install the ' +
                           f'release into a separate virtualenv and run this script with its ' +
                           f'interpreter.')
    out_root = args.out or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        streaming.__version__)
    if os.path.exists(out_root):
        if not args.force:
            raise FileExistsError(f'{out_root} exists; pass --force to replace it.')
        shutil.rmtree(out_root)
    os.makedirs(out_root)

    samples = content.make_samples()
    specs = write_datasets(out_root, samples)

    scratch = tempfile.mkdtemp()
    try:
        clean_stale_shared_memory()
        mds_dir = os.path.join(out_root, 'mds-none')
        orders = record_orders(mds_dir, scratch)
        state = record_state_dict(mds_dir, scratch)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)

    dump_json(os.path.join(out_root, 'order.json'), {'dataset': 'mds-none', 'entries': orders})
    dump_json(os.path.join(out_root, 'state_dict.json'), {'dataset': 'mds-none', **state})
    dump_json(
        os.path.join(out_root, 'manifest.json'), {
            'streaming': streaming.__version__,
            'python': platform.python_version(),
            'numpy': np.__version__,
            'zstd': zstd.version(),
            'num_samples': len(samples),
            'datasets': specs,
        })

    total = 0
    for dirpath, _, filenames in os.walk(out_root):
        for filename in filenames:
            total += os.path.getsize(os.path.join(dirpath, filename))
    print(f'Wrote {out_root} ({total} bytes) with streaming {streaming.__version__}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',
                        help='Fixture directory (default: next to this script, ' +
                        'named after the installed streaming version).')
    parser.add_argument('--force', action='store_true', help='Replace an existing directory.')
    main(parser.parse_args())
