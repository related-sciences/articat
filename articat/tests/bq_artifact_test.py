from datetime import datetime

import pytest

from articat.bq_artifact import BQArtifact


def test_versioned_not_supported():
    with pytest.raises(
        NotImplementedError, match="Versioned BigQuery Artifact is not supported"
    ):
        BQArtifact.versioned("foo", version="0.1.0")


def test_bq_client_is_lazy():
    assert BQArtifact.bq_client


def test_partitioned_must_be_date():
    assert BQArtifact.partitioned("foo")
    with pytest.raises(
        ValueError, match="Partition resolution for BQ artifact must be a day"
    ):
        BQArtifact.partitioned("foo", partition=datetime.utcnow())
