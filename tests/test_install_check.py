# Copyright 2024 MosaicML Streaming authors
# SPDX-License-Identifier: Apache-2.0

import importlib
import importlib.metadata
import warnings
from typing import Any
from unittest.mock import Mock

import pytest

import streaming
from streaming._install_check import CONFLICTING_DISTRIBUTION, warn_if_conflicting_install


def test_warns_when_upstream_distribution_is_installed(monkeypatch: pytest.MonkeyPatch):
    looked_up = []

    def fake_distribution(name: str) -> Any:
        looked_up.append(name)
        return Mock(name='mosaicml_streaming-0.13.0.dist-info')

    monkeypatch.setattr(importlib.metadata, 'distribution', fake_distribution)
    with pytest.warns(RuntimeWarning, match=r'`pip uninstall mosaicml-streaming`') as record:
        warn_if_conflicting_install()
    assert looked_up == [CONFLICTING_DISTRIBUTION]
    assert len(record) == 1
    assert 'reinstall streaming-community' in str(record[0].message)


def test_import_streaming_runs_the_check(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(importlib.metadata, 'distribution', lambda name: Mock())
    with pytest.warns(RuntimeWarning, match=r'`pip uninstall mosaicml-streaming`'):
        importlib.reload(streaming)


def test_no_warning_when_upstream_distribution_is_absent(monkeypatch: pytest.MonkeyPatch):

    def fake_distribution(name: str) -> Any:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, 'distribution', fake_distribution)
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        warn_if_conflicting_install()


def test_no_warning_for_this_distribution_itself():
    # The real lookup against the test environment, where streaming-community is installed:
    # the whole normalised name is compared, so our own distribution never triggers the warning.
    try:
        importlib.metadata.distribution('streaming-community')
    except importlib.metadata.PackageNotFoundError:
        pytest.skip('streaming-community is not installed as a distribution here')
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        warn_if_conflicting_install()


def test_lookup_errors_do_not_break_import(monkeypatch: pytest.MonkeyPatch):

    def fake_distribution(name: str) -> Any:
        raise OSError('unreadable site-packages')

    monkeypatch.setattr(importlib.metadata, 'distribution', fake_distribution)
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        warn_if_conflicting_install()
