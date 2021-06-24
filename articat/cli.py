import importlib
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, Literal, Mapping, Optional

import fire

from articat.artifact import Artifact
from articat.bq_artifact import BQArtifact
from articat.catalog import Catalog
from articat.fs_artifact import FSArtifact
from articat.logging_utils import setup_console_logging

logger = logging.getLogger(__name__)


class CLI:
    """RS Catalog CLI interface"""

    @staticmethod
    def _get_task_class(task_name: str, task_module: str) -> Any:
        task_module_path = Path(task_module)
        if task_module_path.exists():
            if task_module.endswith(".py"):
                task_module = task_module.rstrip(".py")
            task_module_o = importlib.import_module(
                task_module.replace(os.path.sep, ".")
            )
        else:
            task_module_o = importlib.import_module(task_module)

        assert (
            task_name in task_module_o.__dict__
        ), f"Task {task_name} not found in {task_module}"
        return getattr(task_module_o, task_name)

    @classmethod
    def open(
        cls,
        *,
        name: Optional[str] = None,
        module: Optional[str] = None,
        id: Optional[str] = None,
        partition: Optional[str] = None,
        version: Optional[str] = None,
        task_params: Optional[Dict[str, Any]] = {},
    ) -> None:
        """Open URL associated with the Artifact.

        You can specify artifact by either:
         * Task Module + Task Name
         * Artifact ID

        and optionally partition/version. If available using Task route is preferred.
        Task module can be either path to the task module, or module string. Task Name
        is the Task class name.
        :param name: Task class name
        :param module: Path or module of the Task
        :param id: Artifact ID
        :param partition: Optional partition (YYYY-MM-DD)
        :param version: Optional version
        :param task_params: Optional task parameters
        """
        if id is not None:
            artifact = Artifact(id=id, partition=partition, version=version).fetch()
            if hasattr(artifact, "files_pattern"):
                artifact = FSArtifact.parse_obj(artifact)
            elif hasattr(artifact, "table_id"):
                artifact = BQArtifact.parse_obj(artifact)
            else:
                raise ValueError(f"Don't know how to open {artifact}")
        else:
            if name is None or module is None:
                raise ValueError(
                    "You must specify artifact id OR (task_name AND task_module)"
                )
            task_cls = cls._get_task_class(name, module)
            assert isinstance(
                task_params, Mapping
            ), f"task_params should be a dict, it's `{task_params}`"
            artifact = (
                task_cls(
                    version=version,
                    partition=partition and date.fromisoformat(partition),
                    **task_params,
                )
                .output_artifact()
                .fetch()
            )
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


if __name__ == "__main__":
    setup_console_logging()

    fire.Fire(CLI)
