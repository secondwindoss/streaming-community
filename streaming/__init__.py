# Copyright 2022-2024 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

"""MosaicML Streaming Datasets for cloud-native model training."""

from streaming import _install_check

_install_check.warn_if_conflicting_install()

import streaming.multimodal as multimodal
import streaming.text as text
import streaming.vision as vision
from streaming._version import __version__  # noqa: F401
from streaming.base import (CSVWriter, JSONWriter, LocalDataset, MDSWriter, Stream,
                            StreamingDataLoader, StreamingDataset, TSVWriter, XSVWriter)

__all__ = [
    'StreamingDataLoader',
    'Stream',
    'StreamingDataset',
    'CSVWriter',
    'JSONWriter',
    'MDSWriter',
    'TSVWriter',
    'XSVWriter',
    'LocalDataset',
    'multimodal',
    'vision',
    'text',
]
