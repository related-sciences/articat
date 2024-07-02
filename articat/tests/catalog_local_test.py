import pytest

from articat.catalog_local import CatalogLocal
from articat.exceptions import (
    MissingArtifactException,  # Ensure MissingArtifactException is imported
)


def test_catalog_local_missing_artifact_exception():
    """
    Test that `MissingArtifactException` is raised when an artifact cannot be found in `CatalogLocal`.
    """
    catalog = CatalogLocal()
    with pytest.raises(MissingArtifactException) as exc_info:
        catalog.get("non_existing_artifact_id")
    assert "Can't find requested artifact" in str(exc_info.value)
