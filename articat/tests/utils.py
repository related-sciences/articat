import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Iterator, Optional, Type

import fsspec
from google.cloud import datastore

from articat.artifact import ID, Partition, Version
from articat.catalog import Catalog
from articat.config import ArticatConfig
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


if TYPE_CHECKING:
    # NOTE: we want to use TestFSArtifactMixin across multiple
    #       test classes, and flavours of FSArtifact
    BASE_CLASS = FSArtifact
else:
    BASE_CLASS = object


class TestFSArtifactMixin(BASE_CLASS):
    # Note: we overwrite str format in tests to avoid partition path conflicts
    #       for versioned outputs
    _partition_str_format: ClassVar[str] = "%Y%m%dT%H%M%S%f"

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
            assert ArticatConfig.fs_tmp_prefix in a.staging_file_prefix
            a.files_pattern = f"{a.staging_file_prefix}/output.txt"
            yield a

    @classmethod
    def write_dummy_partitioned(
        cls, uid: ID, partition: Partition, dev: bool = False
    ) -> "TestFSArtifact":
        with cls.dummy_partitioned_ctx(uid, partition, dev=dev) as a:
            ...
        return a


class TestFSArtifact(TestFSArtifactMixin, FSArtifact):
    """FSArtifact used for tests"""

    __test__: ClassVar[bool] = False
    """
    This field prevents pytest from interpreting TestFSArtifact as
    a test class. Artifact can also check for the existence of
    __test__ to check if it's running the the test context.
    """
