# articat
[![CI](https://github.com/related-sciences/articat/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/related-sciences/articat/actions/workflows/build.yml)
[![PYPI](https://img.shields.io/pypi/v/articat.svg)](https://pypi.org/project/articat/)

Minimal metadata catalog to store and retrieve metadata about data artifacts.

High level features:
 * set of predefined metadata models
 * flexible metadata (arbitrary key-values)
 * no long running services (low maintenance)
 * IO/data format agnostic
 * immutable metadata

## Example:

To publish a file system Artifact:

```python
from articat.fs_artifact import FSArtifact
from pathlib import Path
from datetime import date

with FSArtifact.partitioned(id="foo", partition=date.today()) as fsa:
    tmp_file = fsa.joinpath("answer.txt")
    Path(tmp_file).write_text("42")
    fsa.metadata.description = "Answer to the Ultimate Question of Life, the Universe, and Everything"
```

To retrieve metadata about the Artifact above:

```python
from articat.fs_artifact import FSArtifact
from datetime import date

FSArtifact.partitioned(id="foo", partition=date.today()).fetch()
```

## Artifact flavours

Currently available Artifact flavours:
 * File System Artifact: FSArtifact
 * BigQuery Artifact: BQArtifact
 * Notebook Artifact: NotebookArtifact
