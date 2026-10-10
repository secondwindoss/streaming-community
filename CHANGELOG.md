# Changelog

streaming-community forked from MosaicML Streaming at upstream commit d99bf9c (25 June 2026), eight commits after the 0.13.0 release. Each entry names the pull request in this repository; imported upstream pull requests name their original author.

## Unreleased

- Rename the distribution to `streaming-community`; the import name stays `streaming` (#18).
- Warn at import time when `mosaicml-streaming` is installed in the same environment, and finish the rename in the install hint, the dataset READMEs and the contributor files (#32).
- Save mode-I images as `I;16` PNG ahead of Pillow 13 (#17).
- Stop parsing Windows drive letters as cloud storage schemes; upstream mosaicml/streaming#986 by @Abhishek21g (#21).
- Fix the `get_shm_prefix` follower race, with a bounded budget for a real `local` mismatch between ranks; upstream mosaicml/streaming#985 by @Abhishek21g (#20).
- Keep nested directories in `merge_index` shard paths; upstream mosaicml/streaming#983 by @discobot (#23).
- Fix non-nullable `ArrayType` columns and forward `exist_ok` in `dataframe_to_mds`; upstream mosaicml/streaming#984 by @discobot and mosaicml/streaming#991 by @SiluPanda (#25).
- Do not evict local-only shards under `cache_limit`; upstream mosaicml/streaming#989 by @gyanu2507 (#27).
- Honor multi-epoch batch durations in the simulator; upstream mosaicml/streaming#990 by @SiluPanda (#24).
- Add golden tests against the datasets, sample orders and `state_dict` values written by 0.13.0 (#22).
- Replace upstream CI with a self-contained workflow on Python 3.10 to 3.14, skip the Spark tests on 3.14, group Dependabot updates into weekly pull requests and fix `test_dataloader_mid_epoch_exit` under `forkserver` (#6, #7, #11, #12).
- Publish releases through PyPI trusted publishing (#19).
- Dependency bumps by Dependabot: setuptools `<85`, xxhash `<5`, google-cloud-storage `<3.17`, the grouped minor and patch updates and the GitHub Actions (#1, #3, #15, #14, #13).
