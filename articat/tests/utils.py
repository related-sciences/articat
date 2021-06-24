import os
import tempfile
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import Iterator, Optional, Type

import fsspec
from google.cloud import datastore

from articat.artifact import ID, Partition, Version
from articat.catalog import Catalog
from articat.fs_artifact import FSArtifact


class TestCatalog(Catalog):
    """Catalog used for tests"""

    @classmethod
    @lru_cache
    def _client(
        cls, project: str = "foobar", namespace: Optional[str] = None
    ) -> datastore.Client:
        os.environ["DATASTORE_EMULATOR_HOST"] = "127.0.0.1:8099"
        return datastore.Client(project=project, namespace=namespace)


class TestFSArtifact(FSArtifact):
    """FSArtifact used for tests"""

    __test__ = False
    """
    This field prevents pytest from interpreting TestFSArtifact as
    a test class. Artifact can also check for the existence of
    __test__ to check if it's running the the test context.
    """

    _tmp_path_prefix: str = tempfile.mkdtemp(prefix="temp_output")
    _path_prefix: str = tempfile.mkdtemp(prefix="final_output")
    _dev_path_prefix = tempfile.mkdtemp(prefix="dev_output")
    # Note: we overwrite str format in tests to avoid partition path conflicts
    #       for versioned outputs
    _partition_str_format: str = "%Y%m%dT%H%M%S%f"

    @classmethod
    def _catalog(cls) -> "Type[Catalog]":
        return TestCatalog

    def __enter__(self) -> FSArtifact:
        a = super().__enter__()
        Path(self.staging_file_prefix).mkdir(parents=True, exist_ok=False)
        return a

    @classmethod
    @contextmanager
    def dummy_versioned_ctx(
        cls, uid: ID, version: Version, dev: bool = False
    ) -> "Iterator[TestFSArtifact]":
        with cls.versioned(uid, version, dev=dev) as a:
            with fsspec.open(a.joinpath("output.txt"), "w") as f:
                f.write("ala ma kota")
            a.files_pattern = f"{a.staging_file_prefix}/output.txt"
            yield a

    @classmethod
    def write_dummy_versioned(
        cls, uid: ID, version: Version, dev: bool = False
    ) -> "TestFSArtifact":
        with cls.dummy_versioned_ctx(uid, version, dev=dev) as a:
            ...
        return a

    @classmethod
    @contextmanager
    def dummy_partitioned_ctx(
        cls, uid: ID, partition: Partition, dev: bool = False
    ) -> "Iterator[TestFSArtifact]":
        with cls.partitioned(uid, partition=partition, dev=dev) as a:
            with fsspec.open(a.joinpath("output.txt"), "w") as f:
                f.write("ala ma kota")
            assert "temp_output" in a.staging_file_prefix
            a.files_pattern = f"{a.staging_file_prefix}/output.txt"
            yield a

    @classmethod
    def write_dummy_partitioned(
        cls, uid: ID, partition: Partition, dev: bool = False
    ) -> "TestFSArtifact":
        with cls.dummy_partitioned_ctx(uid, partition, dev=dev) as a:
            ...
        return a
