import pytest

from articat.catalog_local import CatalogLocal
from articat.exceptions import MissingArtifactException


def test_catalog_local_missing_artifact_exception():
    """
    Test that `MissingArtifactException` is raised when an artifact cannot be found in `CatalogLocal`.
    """
    catalog = CatalogLocal()
    with pytest.raises(MissingArtifactException, match="Can't find requested artifact"):
        catalog.get("non_existing_artifact_id")
