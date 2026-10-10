<br />
<p align="center">
    <a href="https://github.com/mosaicml/streaming#gh-light-mode-only" class="only-light">
      <img src="./docs/source/_static/images/streaming-logo-light-mode.png" width="50%"/>
    </a>
    <!--pypi website does not support dark mode and does not understand GitHub tag. Hence, it renders both the images.
    The below tag is being used to remove the dark mode image on pypi website.-->
    <!-- SETUPTOOLS_LONG_DESCRIPTION_HIDE_BEGIN -->
    <a href="https://github.com/mosaicml/streaming#gh-dark-mode-only" class="only-dark">
      <img src="./docs/source/_static/images/streaming-logo-dark-mode.png" width="50%"/>
    </a>
    <!-- SETUPTOOLS_LONG_DESCRIPTION_HIDE_END -->
</p>

<h1 align="center">streaming-community</h1>

<h2><p align="center">Fast, accurate streaming of training data from cloud storage</p></h2>

<h4><p align='center'>
<a href="https://www.mosaicml.com">[Website]</a>
- <a href="https://docs.mosaicml.com/projects/streaming/en/latest/getting_started/quick_start.html">[Quick Start]</a>
- <a href="https://streaming.docs.mosaicml.com/">[Docs]</a>
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
    <a href="https://streaming.docs.mosaicml.com">
        <img alt="Documentation" src="https://readthedocs.org/projects/streaming/badge/?version=stable">
    </a>
    <a href="https://github.com/secondwindoss/streaming-community/blob/main/LICENSE">
        <img alt="License" src="https://img.shields.io/badge/License-Apache%202.0-green.svg">
    </a>
</p>
<br />

# About this fork

streaming-community is a community fork of [MosaicML Streaming](https://github.com/mosaicml/streaming). The last upstream release, 0.13.0, was published on 2025-07-15 and no release has followed. That release pins `transformers<5`, so it cannot be installed together with transformers 5, and it pins `numpy<2.2`, so with `numpy>=2.2` pip and uv silently fall back to 0.10.0, the last release without the numpy pin. This fork publishes the same library as `streaming-community` with current dependency bounds.

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

Upstream remains at https://github.com/mosaicml/streaming; see [NOTICE](./NOTICE) for attribution.

# 👋 Welcome

MosaicML built StreamingDataset to make training on large datasets from cloud storage as fast, cheap, and scalable as possible.

It’s specially designed for multi-node, distributed training for large models—maximizing correctness guarantees, performance, and ease of use. Now, you can efficiently train anywhere, independent of your training data location. Just stream in the data you need, when you need it. To learn more about why MosaicML built StreamingDataset, read the [announcement blog](https://www.mosaicml.com/blog/mosaicml-streamingdataset).

StreamingDataset is compatible with any data type, including **images, text, video, and multimodal data**.

With support for major cloud storage providers ([AWS](https://aws.amazon.com/s3/), [OCI](https://www.oracle.com/cloud/storage/object-storage/), [GCS](https://cloud.google.com/storage), [Azure](https://azure.microsoft.com/en-us/products/storage/blobs), [Databricks](https://docs.databricks.com/en/storage/index.html), and any S3 compatible object store such as [Cloudflare R2](https://www.cloudflare.com/products/r2/), [Coreweave](https://docs.coreweave.com/storage/object-storage), [Backblaze b2](https://www.backblaze.com/b2/cloud-storage.html), etc. ) and designed as a drop-in replacement for your PyTorch [IterableDataset](https://pytorch.org/docs/stable/data.html#torch.utils.data.IterableDataset) class, StreamingDataset seamlessly integrates into your existing training workflows.

![The flow of samples from shards in the cloud to devices in your cluster](docs/source/_static/images/flow.gif)

# 🚀 Getting Started

## 💾 Installation

Streaming can be installed with `pip`:

<!--pytest.mark.skip-->
```bash
pip install streaming-community
```

## 🏁 Quick Start

### 1. Prepare Your Data

Convert your raw dataset into one of our supported streaming formats:

- MDS (Mosaic Data Shard) format which can encode and decode any Python object
- CSV / TSV
- JSONL

<!--pytest.mark.skip-->
```python
import numpy as np
from PIL import Image
from streaming import MDSWriter

# Local or remote directory in which to store the compressed output files
data_dir = 'path-to-dataset'

# A dictionary mapping input fields to their data types
columns = {
    'image': 'jpeg',
    'class': 'int'
}

# Shard compression, if any
compression = 'zstd'

# Save the samples as shards using MDSWriter
with MDSWriter(out=data_dir, columns=columns, compression=compression) as out:
    for i in range(10000):
        sample = {
            'image': Image.fromarray(np.random.randint(0, 256, (32, 32, 3), np.uint8)),
            'class': np.random.randint(10),
        }
        out.write(sample)
```

### 2. Upload Your Data to Cloud Storage

Upload your streaming dataset to the cloud storage of your choice ([AWS](https://aws.amazon.com/s3/), [OCI](https://www.oracle.com/cloud/storage/object-storage/), or [GCP](https://cloud.google.com/storage)). Below is one example of uploading a directory to an S3 bucket using the [AWS CLI](https://aws.amazon.com/cli/).

<!--pytest.mark.skip-->
```bash
$ aws s3 cp --recursive path-to-dataset s3://my-bucket/path-to-dataset
```

### 3. Build a StreamingDataset and DataLoader

<!--pytest.mark.skip-->
```python
from torch.utils.data import DataLoader
from streaming import StreamingDataset

# Remote path where full dataset is persistently stored
remote = 's3://my-bucket/path-to-dataset'

# Local working dir where dataset is cached during operation
local = '/tmp/path-to-dataset'

# Create streaming dataset
dataset = StreamingDataset(local=local, remote=remote, shuffle=True)

# Let's see what is in sample #1337...
sample = dataset[1337]
img = sample['image']
cls = sample['class']

# Create PyTorch DataLoader
dataloader = DataLoader(dataset)
```

### 📚 What next?

Getting started guides, examples, API references, and other useful information can be found in our [docs](https://streaming.docs.mosaicml.com/).

We have end-to-end tutorials for training a model on:

- [CIFAR-10](https://docs.mosaicml.com/projects/streaming/en/stable/how_to_guides/cifar10.html)
- [FaceSynthetics](https://github.com/secondwindoss/streaming-community/blob/main/examples/facesynthetics.ipynb)
- [SyntheticNLP](https://docs.mosaicml.com/projects/streaming/en/stable/how_to_guides/synthetic_nlp.html)

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

# **🔑** Key Features

---

## Seamless data mixing

Easily experiment with dataset mixtures with [`Stream`](https://docs.mosaicml.com/projects/streaming/en/latest/api_reference/generated/streaming.Stream.html#stream). Dataset sampling can be controlled in relative (proportion) or absolute (repeat or samples terms). During streaming, the different datasets are streamed, shuffled, and mixed seamlessly just-in-time.

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

## True Determinism

A unique feature of our solution: samples are in the same order regardless of the number of GPUs, nodes, or CPU workers. This makes it easier to:

- Reproduce and debug training runs and loss spikes
- Load a checkpoint trained on 64 GPUs and debug on 8 GPUs with reproducibility

See the figure below — training a model on 1, 8, 16, 32, or 64 GPUs yields the **exact same loss curve** (up to the limitations of floating point math!)

![Plot of elastic determinism](docs/source/_static/images/determinism.png)

## Instant mid-epoch resumption

It can be expensive — and annoying — to wait for your job to resume while your dataloader spins after a hardware failure or loss spike. Thanks to our deterministic sample ordering, StreamingDataset lets you resume training in seconds, not hours, in the middle of a long training run.

Minimizing resumption latency can save thousands of dollars in egress fees and idle GPU compute time compared to existing solutions.

## High throughput

Our MDS format cuts extraneous work to the bone, resulting in ultra-low sample latency and higher throughput compared to alternatives for workloads bottlenecked by the dataloader.

| Tool | Throughput |
| --- | --- |
| StreamingDataset | ~19000 img/sec |
| ImageFolder | ~18000 img/sec |
| WebDataset | ~16000 img/sec |

*Results shown are from ImageNet + ResNet-50 training, collected over 5 repetitions after the data is cached after the first epoch.*

## Equal convergence

Model convergence from using StreamingDataset is just as good as using local disk, thanks to our shuffling algorithm.

![Plot of equal convergence](docs/source/_static/images/convergence.png)

Below are results from ImageNet + ResNet-50 training, collected over 5 repetitions.

| Tool | Top-1 Accuracy |
| --- | --- |
| StreamingDataset | 76.51% +/- 0.09 |
| ImageFolder | 76.57% +/- 0.10 |
| WebDataset | 76.23% +/- 0.17 |

StreamingDataset shuffles across all samples assigned to a node, whereas alternative solutions only shuffle samples in a smaller pool (within a single process). Shuffling across a wider pool spreads out adjacent samples more. In addition, our shuffling algorithm minimizes dropped samples. We have found both of these shuffling features advantageous for model convergence.

## Random access

Access the data you need when you need it.

Even if a sample isn’t downloaded yet, you can access `dataset[i]` to get sample `i`. The download will kick off immediately and the result will be returned when it’s done - similar to a map-style PyTorch dataset with samples numbered sequentially and accessible in any order.

<!--pytest.mark.skip-->
```python
dataset = StreamingDataset(...)
sample = dataset[19543]
```

## No divisibility requirements

StreamingDataset will happily iterate over any number of samples. You do not have to forever delete samples so that the dataset is divisible over a baked-in number of devices. Instead, each epoch a different selection of samples are repeated (none dropped) so that each device processes the same count.

<!--pytest.mark.skip-->
```python
dataset = StreamingDataset(...)
dl = DataLoader(dataset, num_workers=...)
```

## Disk usage limits

Dynamically delete least recently used shards in order to keep disk usage under a specified limit. This is enabled by setting the StreamingDataset argument `cache_limit`. See the [shuffling](./docs/source/fundamentals/shuffling.md) guide for more details.

```
dataset = StreamingDataset(
    cache_limit='100gb',
    ...
)
```

# 🏆 Project Showcase

Here are some projects and experiments that used StreamingDataset. Got something to add? Open a pull request.

- [BioMedLM](https://www.mosaicml.com/blog/introducing-pubmed-gpt): a Domain Specific Large Language Model for BioMedicine by MosaicML and Stanford CRFM
- [Mosaic Diffusion Models](https://www.mosaicml.com/blog/training-stable-diffusion-from-scratch-costs-160k): Training Stable Diffusion from Scratch Costs <$160k
- [Mosaic LLMs](https://www.mosaicml.com/blog/gpt-3-quality-for-500k): GPT-3 quality for <$500k
- [Mosaic ResNet](https://www.mosaicml.com/blog/mosaic-resnet): Blazingly Fast Computer Vision Training with the Mosaic ResNet and Composer
- [Mosaic DeepLabv3](https://www.mosaicml.com/blog/mosaic-image-segmentation): 5x Faster Image Segmentation Training with MosaicML Recipes
- …more to come! Stay tuned!

# 💫 Contributors

We welcome any contributions, pull requests, or issues.

To start contributing, see our [Contributing](https://github.com/secondwindoss/streaming-community/blob/main/CONTRIBUTING.md) page.

If you like this project, give us a star **⭐**

# ✍️ Citation

```
@misc{mosaicml2022streaming,
    author = {The Mosaic ML Team},
    title = {streaming},
    year = {2022},
    howpublished = {\url{<https://github.com/mosaicml/streaming/>}},
}
```
