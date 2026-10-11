<br />
<p align="center">
    <a href="https://github.com/mosaicml/streaming#gh-light-mode-only" class="only-light">
      <img src="https://raw.githubusercontent.com/secondwindoss/streaming-community/025e936c4f7d887fd72e93360ca88f815a365564/docs/source/_static/images/streaming-logo-light-mode.png" width="50%"/>
    </a>
    <!--pypi website does not support dark mode and does not understand GitHub tag. Hence, it renders both the images.
    The below tag is being used to remove the dark mode image on pypi website.-->
    <!-- SETUPTOOLS_LONG_DESCRIPTION_HIDE_BEGIN -->
    <a href="https://github.com/mosaicml/streaming#gh-dark-mode-only" class="only-dark">
      <img src="https://raw.githubusercontent.com/secondwindoss/streaming-community/025e936c4f7d887fd72e93360ca88f815a365564/docs/source/_static/images/streaming-logo-dark-mode.png" width="50%"/>
    </a>
    <!-- SETUPTOOLS_LONG_DESCRIPTION_HIDE_END -->
</p>

<h1 align="center">streaming-community</h1>

<h2><p align="center">Fast, accurate streaming of training data from cloud storage</p></h2>

<h4><p align='center'>
<a href="https://www.mosaicml.com">[Website]</a>
- <a href="https://streaming-community.readthedocs.io/en/stable/getting_started/quick_start.html">[Quick Start]</a>
- <a href="https://streaming-community.readthedocs.io/en/stable/">[Docs]</a>
</p></h4>

<p align="center">
    <a href="https://pypi.org/project/mosaicml-streaming/">
        <img alt="PyPi Version" src="https://img.shields.io/pypi/pyversions/mosaicml-streaming">
    </a>
    <a href="https://pypi.org/project/mosaicml-streaming/">
        <img alt="PyPi Package Version" src="https://img.shields.io/pypi/v/mosaicml-streaming">
    </a>
    <a href="https://github.com/secondwindoss/streaming-community/actions/workflows/ci.yaml">
        <img alt="CI" src="https://github.com/secondwindoss/streaming-community/actions/workflows/ci.yaml/badge.svg">
    </a>
    <a href="https://streaming-community.readthedocs.io/en/stable/">
        <img alt="Documentation" src="https://readthedocs.org/projects/streaming-community/badge/?version=stable">
    </a>
    <a href="https://github.com/secondwindoss/streaming-community/blob/main/LICENSE">
        <img alt="License" src="https://img.shields.io/badge/License-Apache%202.0-green.svg">
    </a>
</p>
<br />

<p align="center">
  <b>Stream training data from cloud storage to every GPU: fast, deterministic and resumable.</b><br/>
  The maintained fork of MosaicML Streaming. <code>import streaming</code> and your MDS datasets work unchanged, now with transformers 5, numpy 2.5 and Python 3.14.
</p>

<p align="center">
  <a href="#-installation">Install</a> •
  <a href="#-quick-start">Quick start</a> •
  <a href="#-why-streaming-community">Why this fork</a> •
  <a href="#-key-features">Features</a> •
  <a href="#-storage-and-formats">Storage</a> •
  <a href="#-about-this-fork">Migrating</a> •
  <a href="https://streaming-community.readthedocs.io/en/stable/">Docs</a>
</p>

<p align="center">
  <img alt="The flow of samples from shards in the cloud to devices in your cluster" src="https://raw.githubusercontent.com/secondwindoss/streaming-community/025e936c4f7d887fd72e93360ca88f815a365564/docs/source/_static/images/flow.gif" width="85%"/>
</p>


# ✅ Why streaming-community

- **Drop-in.** Same `import streaming`, same MDS, JSON and CSV/TSV formats, same `state_dict` keys. Golden tests check that datasets, sample order and checkpoints written by 0.13.0 still read the same.
- **Installs with today's stack.** `transformers<6`, `numpy<3`, `huggingface_hub<3` and `pyspark<5`, and CI checks on every change that `[all]` resolves with numpy 2.5 and transformers 5.
- **Tested on Python 3.10 to 3.14** on every pull request.
- **Bugs fixed:** cache eviction no longer deletes local-only shards, the shared-memory prefix race is gone, and file locks and PNG encoding work with filelock 4 and Pillow 13.
- **Released regularly** to PyPI through trusted publishing, with a changelog for every version.


# 💾 Installation

<!--pytest.mark.skip-->
```bash
pip install streaming-community
```

S3 and S3-compatible stores, Google Cloud Storage, Azure, OCI and SFTP work out of the box. Optional extras:

| Extra | Adds |
| --- | --- |
| `hf` | Hugging Face Hub datasets (`hf://`) |
| `databricks` | Databricks Unity Catalog volumes and DBFS (`dbfs:/`) |
| `spark` | `dataframe_to_mds`, converting Spark DataFrames to MDS |
| `alipan` | Alipan (`alipan://`) |
| `simulator` | the `simulator` app for tuning shuffle quality and throughput |

<!--pytest.mark.skip-->
```bash
pip install "streaming-community[hf,databricks]"
```

Moving from `mosaicml-streaming`? Uninstall it first; see [Migrating](#-about-this-fork).


# 🏁 Quick start

Write samples as MDS shards, then stream them into a PyTorch `DataLoader`:

```python
import numpy as np
from torch.utils.data import DataLoader

from streaming import MDSWriter, StreamingDataset

# 1. Write samples as compressed MDS shards. `out` can also be a cloud path such as s3://bucket/dataset.
columns = {'image': 'ndarray:uint8', 'label': 'int'}
with MDSWriter(out='/tmp/streaming-quickstart', columns=columns, compression='zstd', exist_ok=True) as out:
    for i in range(1000):
        out.write({'image': np.random.randint(0, 256, (32, 32, 3), dtype=np.uint8), 'label': i % 10})

# 2. Stream them back. For a cloud dataset, pass remote='s3://bucket/dataset' and a local cache directory.
dataset = StreamingDataset(local='/tmp/streaming-quickstart', batch_size=32, shuffle=True)
batch = next(iter(DataLoader(dataset, batch_size=32)))
print(batch['image'].shape, batch['label'][:8])
```

Next steps: [main concepts](https://streaming-community.readthedocs.io/en/stable/getting_started/main_concepts.html), [converting your dataset](https://streaming-community.readthedocs.io/en/stable/preparing_datasets/basic_dataset_conversion.html), [cloud credentials](https://streaming-community.readthedocs.io/en/stable/how_to_guides/configure_cloud_storage_credentials.html), and end-to-end tutorials for [CIFAR-10](https://streaming-community.readthedocs.io/en/stable/how_to_guides/cifar10.html), [FaceSynthetics](https://github.com/secondwindoss/streaming-community/blob/main/examples/facesynthetics.ipynb) and [SyntheticNLP](https://streaming-community.readthedocs.io/en/stable/how_to_guides/synthetic_nlp.html).


# 🔑 Key features

| | Feature | What you get |
| :---: | --- | --- |
| 🎛️ | [Seamless data mixing](https://streaming-community.readthedocs.io/en/stable/dataset_configuration/mixing_data_sources.html) | Mix any number of `Stream`s by proportion, repeat or sample count, shuffled together just in time. |
| 🎯 | [True determinism](https://streaming-community.readthedocs.io/en/stable/distributed_training/elastic_determinism.html) | The same sample order on 1 or 64 GPUs and any number of nodes or workers, so runs and loss spikes reproduce. |
| ⏯️ | [Instant mid-epoch resumption](https://streaming-community.readthedocs.io/en/stable/distributed_training/fast_resumption.html) | Resume a long run from a checkpoint in seconds instead of replaying the epoch. |
| 🚀 | [High throughput](https://streaming-community.readthedocs.io/en/stable/distributed_training/performance_tuning.html) | The MDS format keeps per-sample overhead minimal for dataloader-bound workloads. |
| 📈 | Equal convergence | Shuffling across all samples on a node converges like training from local disk. |
| 🔍 | [Random access](https://streaming-community.readthedocs.io/en/stable/getting_started/main_concepts.html) | `dataset[i]` works even before sample `i` is downloaded. |
| 🧮 | No divisibility requirements | Any number of samples on any number of devices; samples are repeated, never dropped. |
| 💽 | [Disk usage limits](https://streaming-community.readthedocs.io/en/stable/dataset_configuration/shard_retrieval.html) | `cache_limit='100gb'` evicts least recently used shards to stay under budget. |

<details>
<summary><b>Mixing datasets in code</b></summary>

<!--pytest.mark.skip-->
```python
# mix C4, github code, and internal datasets
streams = [
  Stream(remote='s3://datasets/c4', proportion=0.4),
  Stream(remote='s3://datasets/github', proportion=0.1),
  Stream(remote='gcs://datasets/my_internal', proportion=0.5),
]

dataset = StreamingDataset(
  streams=streams,
  samples_per_epoch=1e8,
)
```

</details>

<details>
<summary><b>Determinism, convergence and throughput results</b></summary>

Results measured for the original MosaicML Streaming on ImageNet with ResNet-50, collected over 5 repetitions.

Training on 1, 8, 16, 32 or 64 GPUs yields the exact same loss curve, up to the limits of floating-point math:

![Plot of elastic determinism](https://raw.githubusercontent.com/secondwindoss/streaming-community/025e936c4f7d887fd72e93360ca88f815a365564/docs/source/_static/images/determinism.png)

Convergence matches training from local disk:

![Plot of equal convergence](https://raw.githubusercontent.com/secondwindoss/streaming-community/025e936c4f7d887fd72e93360ca88f815a365564/docs/source/_static/images/convergence.png)

| Tool | Throughput | Top-1 accuracy |
| --- | --- | --- |
| StreamingDataset | ~19000 img/sec | 76.51% +/- 0.09 |
| ImageFolder | ~18000 img/sec | 76.57% +/- 0.10 |
| WebDataset | ~16000 img/sec | 76.23% +/- 0.17 |

Throughput is measured after the data is cached in the first epoch.

</details>


# 🌐 Storage and formats

| Storage | Remote path | Install |
| --- | --- | --- |
| Amazon S3 and S3-compatible stores (set `S3_ENDPOINT_URL`) | `s3://bucket/path` | included |
| Google Cloud Storage | `gs://bucket/path` | included |
| Azure Blob Storage / Azure Data Lake | `azure://` / `azure-dl://` | included |
| Oracle Cloud Infrastructure | `oci://bucket/path` | included |
| SFTP | `sftp://host/path` | included |
| Hugging Face Hub | `hf://datasets/org/name` | `[hf]` |
| Databricks Unity Catalog / DBFS | `dbfs:/Volumes/...` / `dbfs:/...` | `[databricks]` |
| Alipan | `alipan://path` | `[alipan]` |
| Local disk | `/path/to/dataset` | included |

Shard formats: MDS (recommended), JSON and CSV/TSV. See [dataset formats](https://streaming-community.readthedocs.io/en/stable/preparing_datasets/dataset_format.html).

# 🧪 Compatibility

| | Supported | Checked in CI |
| --- | --- | --- |
| Python | 3.10 to 3.14 | full test suite on each version |
| PyTorch | `>=1.10,<3` | current release, CPU |
| transformers | `>=4.21.3,<6` | resolves with transformers 5 |
| numpy | `>=1.21.5,<3` | resolves with numpy 2.5 |


# 🔀 About this fork

streaming-community is a community fork of [MosaicML Streaming](https://github.com/mosaicml/streaming). The last upstream release, 0.13.0, was published on 2025-07-15 and no release has followed. That release pins `transformers<5`, so it cannot be installed together with transformers 5, and it pins `numpy<2.2`, so with `numpy>=2.2` pip and uv silently fall back to 0.10.0, the last release without the numpy pin. This fork publishes the same library as `streaming-community` with current dependency bounds.

<details>
<summary><b>What stays the same, and migrating from <code>mosaicml-streaming</code></b></summary>

What stays the same:

- the import name, `import streaming`
- the MDS, JSON and XSV shard formats and `index.json` version 2
- the `state_dict` keys used for mid-epoch resumption
- the environment variable names

Migrating from `mosaicml-streaming`:

1. Uninstall the upstream package first. Both distributions install into the `streaming/` package directory, so they cannot be installed side by side.

   <!--pytest.mark.skip-->
   ```bash
   pip uninstall mosaicml-streaming
   pip install streaming-community
   ```

2. Projects that pin `mosaicml-streaming`, such as composer `<1.0` and llm-foundry `>=0.12.0,<0.13`, would pull the upstream package back in. With uv, drop their requirement with an override in `pyproject.toml`:

   ```toml
   [tool.uv]
   override-dependencies = ["mosaicml-streaming ; sys_platform == 'never'"]
   ```

   pip has no dependency override. Install such a project with `pip install --no-deps` and add its other requirements yourself.

</details>

Upstream remains at https://github.com/mosaicml/streaming; see [NOTICE](./NOTICE) for attribution.

<details>
<summary><b>Starter code for popular datasets</b></summary>

We also have starter code for the following popular datasets, which can be found in the `streaming` [directory](https://github.com/secondwindoss/streaming-community/tree/main/streaming):

| Dataset | Task | Read | Write |
| --- | --- | --- | --- |
| LAION-400M | Text and image | [Read](https://github.com/mosaicml/diffusion-benchmark/blob/main/data.py) | [Write](https://github.com/secondwindoss/streaming-community/tree/main/streaming/multimodal/convert/laion/laion400m) |
| WebVid | Text and video | [Read](https://github.com/secondwindoss/streaming-community/blob/main/streaming/multimodal/webvid.py) | [Write](https://github.com/secondwindoss/streaming-community/blob/main/streaming/multimodal/convert/webvid.py) |
| C4 | Text | [Read](https://github.com/secondwindoss/streaming-community/blob/main/streaming/text/c4.py) | [Write](https://github.com/secondwindoss/streaming-community/blob/main/streaming/text/convert/c4.py) |
| EnWiki | Text | [Read](https://github.com/secondwindoss/streaming-community/blob/main/streaming/text/enwiki.py) | [Write](https://github.com/secondwindoss/streaming-community/tree/main/streaming/text/convert/enwiki) |
| Pile | Text | [Read](https://github.com/secondwindoss/streaming-community/blob/main/streaming/text/pile.py) | [Write](https://github.com/secondwindoss/streaming-community/blob/main/streaming/text/convert/pile.py)
| ADE20K | Image segmentation | [Read](https://github.com/secondwindoss/streaming-community/blob/main/streaming/vision/ade20k.py) | [Write](https://github.com/secondwindoss/streaming-community/blob/main/streaming/vision/convert/ade20k.py)
| CIFAR10 | Image classification | [Read](https://github.com/secondwindoss/streaming-community/blob/main/streaming/vision/cifar10.py) | [Write](https://github.com/secondwindoss/streaming-community/blob/main/streaming/vision/convert/cifar10.py) |
| COCO | Image classification | [Read](https://github.com/secondwindoss/streaming-community/blob/main/streaming/vision/coco.py) | [Write](https://github.com/secondwindoss/streaming-community/blob/main/streaming/vision/convert/coco.py) |
| ImageNet | Image classification | [Read](https://github.com/secondwindoss/streaming-community/blob/main/streaming/vision/imagenet.py) | [Write](https://github.com/secondwindoss/streaming-community/blob/main/streaming/vision/convert/imagenet.py) |

**To start training on these datasets:**

1. Convert raw data into .mds format using the corresponding script from the `convert` directory.

For example:

<!--pytest.mark.skip-->
```bash
$ python -m streaming.multimodal.convert.webvid --in <CSV file> --out <MDS output directory>
```

2. Import dataset class to start training the model.

<!--pytest.mark.skip-->
```python
from streaming.multimodal import StreamingInsideWebVid
dataset = StreamingInsideWebVid(local=local, remote=remote, shuffle=True)
```

</details>

<details>
<summary><b>🏆 Project showcase</b></summary>

Here are some projects and experiments that used StreamingDataset. Got something to add? Open a pull request.

- [BioMedLM](https://www.mosaicml.com/blog/introducing-pubmed-gpt): a Domain Specific Large Language Model for BioMedicine by MosaicML and Stanford CRFM
- [Mosaic Diffusion Models](https://www.mosaicml.com/blog/training-stable-diffusion-from-scratch-costs-160k): Training Stable Diffusion from Scratch Costs <$160k
- [Mosaic LLMs](https://www.mosaicml.com/blog/gpt-3-quality-for-500k): GPT-3 quality for <$500k
- [Mosaic ResNet](https://www.mosaicml.com/blog/mosaic-resnet): Blazingly Fast Computer Vision Training with the Mosaic ResNet and Composer
- [Mosaic DeepLabv3](https://www.mosaicml.com/blog/mosaic-image-segmentation): 5x Faster Image Segmentation Training with MosaicML Recipes
- …more to come! Stay tuned!

</details>

# 💫 Community

We welcome contributions, pull requests and issues.

- Questions and ideas: [Discussions](https://github.com/secondwindoss/streaming-community/discussions)
- Bugs and feature requests: [Issues](https://github.com/secondwindoss/streaming-community/issues)
- Contributing: [CONTRIBUTING.md](https://github.com/secondwindoss/streaming-community/blob/main/CONTRIBUTING.md)
- Security reports: [SECURITY.md](https://github.com/secondwindoss/streaming-community/blob/main/SECURITY.md)
- Release notes: [CHANGELOG.md](https://github.com/secondwindoss/streaming-community/blob/main/CHANGELOG.md)

If you like this project, give us a star **⭐**

<details>
<summary><b>✍️ Citation</b></summary>

```
@misc{mosaicml2022streaming,
    author = {The Mosaic ML Team},
    title = {streaming},
    year = {2022},
    howpublished = {\url{<https://github.com/mosaicml/streaming/>}},
}
```

</details>
