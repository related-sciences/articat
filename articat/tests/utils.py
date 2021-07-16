import os
from contextlib import contextmanager
from datetime import date, timedelta
from functools import lru_cache
from random import shuffle
from typing import TYPE_CHECKING, ClassVar, Iterator, Optional, Type, TypeVar

import fsspec
from google.cloud import datastore

from articat.artifact import ID, Partition, Version
from articat.catalog import Catalog
from articat.catalog_datastore import CatalogDatastore
from articat.config import ArticatConfig
from articat.fs_artifact import FSArtifact

T = TypeVar("T", bound="TestFSArtifactMixin")


class TestCatalog(CatalogDatastore):
    """Datastore Catalog used for tests"""

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

    @classmethod
    @contextmanager
    def dummy_versioned_ctx(
        cls: Type[T], uid: ID, version: Version, dev: bool = False
    ) -> Iterator[T]:
        with cls.versioned(uid, version, dev=dev) as a:
            with fsspec.open(a.joinpath("output.txt"), "w") as f:
                f.write("ala ma kota")
            a.files_pattern = f"{a.staging_file_prefix}/output.txt"
            yield a

    @classmethod
    def write_dummy_versioned(
        cls: Type[T], uid: ID, version: Version, dev: bool = False
    ) -> T:
        with cls.dummy_versioned_ctx(uid, version, dev=dev) as a:
            ...
        return a

    @classmethod
    @contextmanager
    def dummy_partitioned_ctx(
        cls: Type[T], uid: ID, partition: Partition, dev: bool = False
    ) -> Iterator[T]:
        with cls.partitioned(uid, partition=partition, dev=dev) as a:
            with fsspec.open(a.joinpath("output.txt"), "w") as f:
                f.write("ala ma kota")
            assert ArticatConfig.fs_tmp_prefix() in a.staging_file_prefix
            a.files_pattern = f"{a.staging_file_prefix}/output.txt"
            yield a

    @classmethod
    def write_dummy_partitioned(
        cls: Type[T], uid: ID, partition: Partition, dev: bool = False
    ) -> T:
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


def write_a_couple_of_partitions(uid: ID, n: int) -> None:
    # we shuffle the partitions to further tests ordering etc
    ns = list(range(n))
    shuffle(ns)
    for i in ns:
        dt = date.today() - timedelta(days=i)
        TestFSArtifact.write_dummy_partitioned(uid, dt)
        # write some unrelated dataset
        TestFSArtifact.write_dummy_partitioned(f"{uid}1234", dt)
