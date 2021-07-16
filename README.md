# articat
[![CI](https://github.com/related-sciences/articat/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/related-sciences/articat/actions/workflows/build.yml)
[![PYPI](https://img.shields.io/pypi/v/articat.svg)](https://pypi.org/project/articat/)

Minimal metadata catalog to store and retrieve metadata about data artifacts.

## Getting started

To publish a file system Artifact (`FSArtifact`):

```python
from articat import FSArtifact
from pathlib import Path
from datetime import date

# Apart from being metadata containers, Artifact classes have optional
# convenience methods to help in data publishing:

with FSArtifact.partitioned("foo", partition=date(1643, 1, 4)) as fsa:
    # To create a new Artifact, always use `with` statement, and
    # either `partitioned` or `versioned` methods. Use:
    # * `partitioned(...)`, for Artifacts with explicit `datetime` partition
    # * `versioned(...)`, for Artifacts with explicit `str` version

    # Next we produce some local data, this could be a Spark job,
    # ML model etc.
    data_path = Path("/tmp/data")
    data_path.write_text("42")

    # Now let's stage that data, temporary and final data directories/buckets
    # are configurable
    fsa.stage(data_path)

    # Additionally let's provide some description:
    fsa.metadata.description = "Answer to the Ultimate Question of Life, the Universe, and Everything"
```

To retrieve metadata about the Artifact above:

```python
from articat.fs_artifact import FSArtifact
from datetime import date

# To retrieve metadata, use Artifact object, and `fetch` method:
fsa = FSArtifact.partitioned("foo", partition=date(1643, 1, 4)).fetch()

fsa.id # "foo"
fsa.created # <CREATION-TIMESTAMP>
fsa.partition # <CREATION-TIMESTAMP>
fsa.metadata.description # "Answer to the Ultimate Question of Life, the Universe, and Everything"
fsa.main_dir # Data directory, this is where the data was stored after staging
```

## Features

 * store and retrieve metadata about your data artifacts
 * no long running services (low maintenance)
 * IO/data format agnostic
 * immutable metadata

## Artifact flavours

Currently available Artifact flavours:
 * `FSArtifact`: metadata/utils for files or objects (supports: local FS, GCS, S3 and more)
 * `BQArtifact`: metadata/utils for BigQuery tables
 * `NotebookArtifact`: metadata/utils for Jupyter Notebooks

## Mode

 * `local`: mostly for testing, metadata is stored in locally (configurable, default: `~/.config/articat/local`)
 * `gcp_datastore`: metadata is stored in the Google Cloud Datastore

## Configuration

`articat` configuration can be provided in the API, or configuration files. By default configuration
is loaded from `~/.config/articat/articat.cfg` and `articat.cfg` in current working directory.

You use `local` mode without configuration file. Available options:

 ```toml
[main]
# local or gcp_datastore, default: local
# mode =

# local DB directory, default: ~/.config/articat/local
# local_db_dir =

[fs]
# temporary directory/prefix
# tmp_prefix =
# development data directory/prefix
# dev_prefix =
# production data directory/prefix
# prod_prefix =

[gcp]
# GCP project
# project =

[bq]
# development data BigQuery dataset
# dev_dataset =
# production data BigQuery dataset
# prod_dataset =
```
