# Contributing

streaming-community is a community fork of MosaicML Streaming. Bug reports, fixes and small, focused features are welcome. For anything larger, open an issue first so the approach can be agreed before the work is done.

## Setting up

Clone the repository, create a virtual environment, install the package with the development extras and install the pre-commit hooks. The commands below use [uv](https://docs.astral.sh/uv/); `python -m venv` and `pip` work the same way.

<!--pytest.mark.skip-->
```bash
uv venv
uv pip install -e '.[dev]'
pre-commit install
```

The hooks format and lint every commit (ruff, yapf, isort, docformatter, yamllint and a secret scan). To run them by hand, use `pre-commit run --files <changed files>` or `pre-commit run --all-files`. Pyright runs as its own CI job; `pre-commit run pyright --all-files` runs it locally.

The `dev` extra covers most tests. The cloud, Spark and simulator dependencies are separate extras, and `pip install -e '.[all]'` installs everything, which is what CI tests with.

## Running tests

Run `pytest` with the files you changed, for example `pytest tests/test_writer.py`. The default options in `pyproject.toml` already deselect the `daily` and `remote` markers, so if you override them, pass `-m 'not daily and not remote'` yourself; the remote tests need cloud credentials. `pytest` with no arguments runs the whole suite and takes a while.

Three things to know:

- The Spark tests (`tests/base/converters/` and the `merge_index` tests in `tests/test_util.py`) start a local Spark session and need a JVM that PySpark supports. Without one they fail or hang; skip them with `--ignore=tests/base/converters -k 'not merge_index'` and rely on CI.
- Tests that fork worker processes on macOS need `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` in the environment.
- Streaming keeps its shared memory names and lock files under the temp directory, so two test sessions on one machine interfere with each other. Give each session its own empty `TMPDIR`.

## How changes land

Work on a branch and open a pull request against `main`. CI runs the lint hooks, an install check and the test suite on Python 3.10 to 3.14, and a pull request merges once CI is green and the change has been reviewed. Keep each pull request to one change, and write commit subjects as one sentence that says what the change does. In the description, say what was wrong, list the concrete changes, and say what you ran to check them.

Fixes imported from upstream pull requests keep their original commits and authors; the description says which upstream pull request is imported. Entries for merged changes go into [CHANGELOG.md](CHANGELOG.md) under `Unreleased`.

## Compatibility contract

This fork publishes the same library under a new distribution name. The following stay as upstream left them, and a change to any of them is a compatibility break:

- the import name, `import streaming`;
- the MDS, JSON and XSV shard formats and `index.json` version 2;
- the `state_dict` keys used for mid-epoch resumption (`epoch`, `sample_in_epoch`, `num_canonical_nodes`, `shuffle_seed` and `initial_physical_nodes`);
- the environment variable names.

`tests/test_golden.py` guards this against fixtures that the last upstream release, 0.13.0, wrote. If a golden test fails, fix the change; never regenerate the fixtures to make it pass.

## Reporting security issues

Do not open a public issue for a vulnerability. See [SECURITY.md](SECURITY.md) for how to report one privately.

## Conduct

Contributors are expected to follow the [code of conduct](CODE_OF_CONDUCT.md).
