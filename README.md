# articat
Minimal metadata catalog to store and retrieve metadata about data artifacts.

High level features:
 * set of predefined metadata models
 * flexible metadata (arbitrary key-values)
 * no long running services (low maintenance)
 * IO/data format agnostic
 * immutable metadata
 
## Example:

```python
from articat.fs_artifact import FSArtifact
from pathlib import Path

with FSArtifact.partitioned("foo") as fsa:
    tmp_file = fsa.joinpath("answer.txt")
    Path(tmp_file).write_text("42")
    fsa.metadata.description = "Answer to the Ultimate Question of Life, the Universe, and Everything"
```

```python
from articat.fs_artifact import FSArtifact

FSArtifact.partitioned("foo").fetch()
```
