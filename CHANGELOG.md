# Changelog

streaming-community is a fork of MosaicML Streaming 0.13.0. Each entry names the pull request in this repository.

## Unreleased

### Fixes

- Start DataLoader workers with `fork` after the parent iterated the dataset: a forked child inherited the parent's thread pool without its threads and waited forever. A dataset that is in, or has finished, an epoch also pickles again, so `spawn` and `forkserver` workers no longer fail on it either (#48).
- Raise the error of a failed download thread from iteration, with the cause attached, instead of ending the epoch early or waiting forever for the thread. In 0.14.0, a `cache_limit` too tight for a mix of local-only and remote streams hung or returned an empty epoch this way; that case now fails at construction with the limit to set. The ready thread also downloads a shard again if it was evicted while being waited for, and stops at interpreter exit, so a stalled epoch no longer blocks shutdown (#49).
- Close each shared memory block that the prefix lookup opens to read another dataset's local directories. Every probe stayed open until the process exited, two file descriptors each, so they piled up in a process that creates many datasets (#54).
- Return a shard to its previous state when its download or decompression raises. It stayed `PREPARING`, so every worker waiting for it waited forever and no later call downloaded it again; now the next call retries (#54).
- Delete a downloaded shard file that fails its checksum. It stayed in the cache, so a retry, or the next read of that shard, used the corrupt file without checking it again; now the next attempt downloads it again. A local directory that holds the dataset itself is left alone (#56).

### Docs

- Shared links show a project card: a social preview image in `.github/`, and Open Graph tags on the documentation through `sphinxext-opengraph` (#47).
- Point the torchmetrics cross-reference inventory at its new address on lightning.ai. The old one now ends in a 404, which failed the documentation build (#55).

## 0.14.1 (2026-10-11)

### Docs

- Install `streaming-community` instead of `mosaicml-streaming` in the docs and the tutorial notebooks (#45).

## 0.14.0 (2026-10-11)

First release under the `streaming-community` name. The import name stays `streaming`, and the shard formats, `index.json`, `state_dict` keys and environment variables are unchanged from 0.13.0.

### Packaging

- Rename the distribution to `streaming-community`; the import name stays `streaming` (#18).
- Warn at import time when `mosaicml-streaming` is installed in the same environment, and finish the rename in the install hint, the dataset READMEs and the contributor files (#32).
- Publish releases through PyPI trusted publishing (#19).
- Point the README logo and images at absolute URLs so they render on PyPI (#35, #42), point the README docs links at the streaming-community docs and remove the code of conduct, which linked to MosaicML's community guidelines (#41).

### Dependencies

- Allow numpy 2.5 (`numpy<3`) and huggingface_hub 2 (`huggingface_hub<3` in the `hf` extra) (#29). Upstream `main` already allowed transformers 5 (`transformers<6`).
- Allow pyspark 4 in the `spark` extra (`pyspark<5`), and check in CI that `[all]` resolves with numpy 2.5 and transformers 5 (#31).
- Dependency bumps by Dependabot: setuptools `<85`, xxhash `<5`, google-cloud-storage `<3.17`, the grouped minor and patch updates (among them databricks-sdk 0.147.0) and the GitHub Actions (#1, #3, #15, #14, #13).
- Dependabot updates before the release: databricks-sdk 0.148.0 in the `databricks` extra, docutils `<0.24` in the `docs` extra, and for development pytest 9.1.1 (fixes the `/tmp/pytest-of-{user}` handling in CVE-2025-71176), pytest-cov `<8`, pre-commit `<5` and fastapi 0.142.4, with `tests/test_mixing.py` adjusted for pytest 9 (#16, #37, #38, #39, #40, #43).

### Fixes

- Recreate file locks after fork, so `fork` DataLoader workers work with filelock 4 (#26).
- Save mode-I images as `I;16` PNG ahead of Pillow 13 (#17).
- Do not evict local-only shards under `cache_limit` (#27).
- Fix the `get_shm_prefix` follower race, with a bounded budget for a real `local` mismatch between ranks (#20).
- Stop parsing Windows drive letters as cloud storage schemes (#21).
- Keep nested directories in `merge_index` shard paths (#23).
- Fix non-nullable `ArrayType` columns and forward `exist_ok` in `dataframe_to_mds` (#25).
- Honor multi-epoch batch durations in the simulator (#24).

### Docs

- Move the docs to Sphinx 8 and build them with warnings as errors, replace MosaicML's PostHog analytics with this project's own, use the theme's built-in search instead of MosaicML's Algolia index, point the notebook and Colab links at this repository, and build the docs in CI (#36). The docs are published at https://streaming-community.readthedocs.io.

### Tests and CI

- Add golden tests against the datasets, sample orders and `state_dict` values written by 0.13.0 (#22).
- Replace the inherited CI with a self-contained workflow on Python 3.10 to 3.14, group Dependabot updates into weekly pull requests and fix `test_dataloader_mid_epoch_exit` under `forkserver` (#6, #7, #11, #12). Python 3.14 runs the full suite, Spark tests included, since #31.
- Bump pyright to 1.1.414, fix the errors it reports and make the check blocking (#28).
- Seed `test_balance` so its statistical bound stops failing by chance (#30), use the `fork` start method for the Python 3.14 test session (#33), and run docformatter as a local hook so pre-commit 4 loads the config (#34).
