# Copyright 2024 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

"""Detect the upstream distribution installed next to this one."""

import warnings
from importlib import metadata

__all__ = ['CONFLICTING_DISTRIBUTION', 'warn_if_conflicting_install']

# ``importlib.metadata`` normalises this name the same way it normalises the ``.dist-info``
# directory names it finds, so ``mosaicml_streaming-0.13.0.dist-info`` matches. The whole name is
# compared, not a prefix, so ``streaming_community`` itself never matches.
CONFLICTING_DISTRIBUTION = 'mosaicml-streaming'


def warn_if_conflicting_install() -> None:
    """Warn when ``mosaicml-streaming`` is installed in the same environment.

    Both distributions install into the ``streaming/`` package directory, so whichever was
    installed last overwrote the other's files, and uninstalling either one deletes files the other
    still needs. This costs a single ``importlib.metadata`` lookup and never raises.
    """
    try:
        metadata.distribution(CONFLICTING_DISTRIBUTION)
    except metadata.PackageNotFoundError:
        return
    except Exception:  # Broken metadata must not break ``import streaming``.
        return
    message = (f'{CONFLICTING_DISTRIBUTION} is installed in the same environment as ' +
               'streaming-community and both write to the same `streaming/` package directory, ' +
               f'so run `pip uninstall {CONFLICTING_DISTRIBUTION}` and then reinstall ' +
               'streaming-community.')
    warnings.warn(message, RuntimeWarning, stacklevel=2)
