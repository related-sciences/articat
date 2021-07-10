import logging
import sys
from datetime import date
from pathlib import Path
from typing import Literal, Optional

from articat.artifact import Artifact
from articat.bq_artifact import BQArtifact
from articat.catalog import Catalog
from articat.fs_artifact import FSArtifact

logger = logging.getLogger(__name__)


class CLI:
    """Articat CLI interface"""

    @classmethod
    def open(
        cls,
        *,
        id: Optional[str] = None,
        partition: Optional[str] = None,
        version: Optional[str] = None,
    ) -> None:
        """
        Open URL associated with the Artifact.

        :param id: Artifact ID
        :param partition: Optional partition (YYYY-MM-DD)
        :param version: Optional version
        """
        artifact = Artifact(id=id, partition=partition, version=version).fetch()
        if hasattr(artifact, "files_pattern"):
            artifact = FSArtifact.parse_obj(artifact)
        elif hasattr(artifact, "table_id"):
            artifact = BQArtifact.parse_obj(artifact)
        else:
            raise ValueError(f"Don't know how to open {artifact}")
        logger.info(f"Opening artifact: {artifact.spec()} via {artifact.browser_url()}")
        artifact.open_browser()

    @staticmethod
    def lookup(
        *,
        id: Optional[str] = None,
        partition: Optional[str] = None,
        version: Optional[str] = None,
        limit: Optional[int] = None,
        no_trunc: bool = False,
        dev: bool = False,
        format: Literal["csv", "json"] = "json",
    ) -> None:
        """
        Does RS Catalog lookup and prints results. All parameters are exact, case sensitive match.
        :param id: artifact id
        :param partition: artifact partition date (YYYY-MM-DD)
        :param version: artifact version
        :param limit: limit the number of results
        :param no_trunc: whether to print full metadata or a short version (default: True)
        :param dev: switch to dev mode (default: False)
        :param format: output format, default to json, can also be csv
        """
        assert format in ("json", "csv"), "format has to be one of: json, csv"
        partition_dt = date.fromisoformat(partition) if partition else None
        csv_result = []
        json_exclude = (
            None
            if no_trunc
            else {
                "metadata": ...,
            }
        )
        for e in Catalog.lookup(
            id=id,
            partition_dt_start=partition_dt or date.min,
            partition_dt_end=partition_dt,
            version=str(version) if version else None,
            limit=limit,
            model=Artifact,
            dev=dev,
        ):
            if format == "json":
                print(e.json(exclude=json_exclude), flush=True)
            else:
                assert format == "csv"
                csv_result.append(e.dict(exclude=dict(metadata=...)))
        if len(csv_result) > 0:
            import pandas as pd

            pd.json_normalize(csv_result, sep="_").to_csv(sys.stdout, index=False)

    @staticmethod
    def dump(
        *,
        local_path: str,
        id: str,
        version: str,
        description: str,
        dev: bool = False,
        _test_artifact: Optional[FSArtifact] = None,
    ) -> FSArtifact:
        """
        Ad-hoc dump of local data into the Catalog, creates a versioned FSArtifact.

        :param local_path: local path to dump, can be a file or a directory (directories are recursively uploaded)
        :param id: id of the artifact
        :param version: version
        :param description: meaningful description
        :param dev: whether to use dev mode
        :return: Created FSArtifact
        """
        artifact = _test_artifact or FSArtifact.versioned(
            id=id, version=version, dev=dev
        )
        with artifact as artifact:
            artifact.stage(Path(local_path))
            artifact.metadata.description = description
        return artifact
