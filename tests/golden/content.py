# Copyright 2024 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

"""Deterministic sample content shared by the golden fixture generator and the golden tests.

The generator runs under an installed release of streaming and the tests run against the checkout,
so this module must not import ``streaming``. Values are derived from a pure-Python splitmix64
stream so they are reproducible bit for bit regardless of the numpy version.
"""

from typing import Any, Iterator

import numpy as np

__all__ = ['JSON_COLUMNS', 'MDS_COLUMNS', 'NUM_SAMPLES', 'XSV_COLUMNS', 'make_samples', 'project']

NUM_SAMPLES = 64
SEED = 0x5EED

# Column name -> encoding, covering fixed-size, variable-size, numpy and stringified MDS encodings.
MDS_COLUMNS = {
    'id': 'int',
    'text': 'str',
    'blob': 'bytes',
    'vec': 'ndarray:int32:4',
    'score': 'float64',
    'big': 'str_int',
    'meta': 'json',
    'flag': 'uint8',
}

# The JSON and XSV writers only accept str, int and float columns.
JSON_COLUMNS = {'id': 'int', 'text': 'str', 'score': 'float'}
XSV_COLUMNS = {'id': 'int', 'text': 'str', 'score': 'float'}

# No comma, tab or newline in any word, so the same text column works for CSV and TSV too.
_WORDS = ('alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf', 'hotel', 'india',
          'juliet', 'kilo', 'lima', 'über', 'naïve', '日本語', 'ω')

_MASK64 = (1 << 64) - 1


def _splitmix64(seed: int) -> Iterator[int]:
    """Yield an endless stream of 64-bit values from a splitmix64 generator.

    Args:
        seed (int): Seed.

    Returns:
        Iterator[int]: Pseudo-random 64-bit unsigned integers.
    """
    state = seed & _MASK64
    while True:
        state = (state + 0x9E3779B97F4A7C15) & _MASK64
        z = state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK64
        yield z ^ (z >> 31)


def _make_sample(index: int, rng: Iterator[int]) -> dict[str, Any]:
    """Build one sample from the shared random stream.

    Args:
        index (int): Sample index, stored in the ``id`` column.
        rng (Iterator[int]): The shared 64-bit random stream.

    Returns:
        Dict[str, Any]: Sample with every MDS column.
    """
    num_words = 3 + index % 5
    text = ' '.join(_WORDS[next(rng) % len(_WORDS)] for _ in range(num_words))

    blob = bytes(next(rng) & 0xFF for _ in range(1 + (index * 7) % 13))

    vec = np.array([(next(rng) % (1 << 32)) - (1 << 31) for _ in range(4)], dtype=np.int32)

    # Dyadic rationals divide exactly, so the float is the same on every platform.
    score = ((next(rng) % 2_000_001) - 1_000_000) / 1024.0

    # Larger than int64 on purpose: str_int stores digits, not a fixed-width integer.
    big = (next(rng) << 40) | next(rng)
    if index % 3 == 2:
        big = -big

    meta = {
        'i': index,
        'tags': [_WORDS[next(rng) % len(_WORDS)] for _ in range(index % 3)],
        'ok': bool(next(rng) & 1),
        'nested': {
            'x': next(rng) % 1000
        },
        'none': None,
    }

    flag = next(rng) % 256

    return {
        'id': index,
        'text': text,
        'blob': blob,
        'vec': vec,
        'score': score,
        'big': big,
        'meta': meta,
        'flag': flag,
    }


def make_samples(num_samples: int = NUM_SAMPLES) -> list[dict[str, Any]]:
    """Generate the golden samples.

    Args:
        num_samples (int): Number of samples. Defaults to ``NUM_SAMPLES``.

    Returns:
        List[Dict[str, Any]]: Samples in ``id`` order, each with every MDS column.
    """
    rng = _splitmix64(SEED)
    return [_make_sample(index, rng) for index in range(num_samples)]


def project(sample: dict[str, Any], columns: dict[str, str]) -> dict[str, Any]:
    """Keep only the columns a format supports.

    Args:
        sample (Dict[str, Any]): A sample from :func:`make_samples`.
        columns (Dict[str, str]): Column name -> encoding of the target format.

    Returns:
        Dict[str, Any]: The sample restricted to ``columns``.
    """
    return {name: sample[name] for name in columns}
