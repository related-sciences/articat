from __future__ import annotations

import logging
import os
import typing
from datetime import date, datetime
from os import environ
from types import TracebackType
from typing import Any, ClassVar, Mapping, Optional, Type, TypeVar, Union

from google.cloud import datastore
from google.cloud.datastore.helpers import entity_to_protobuf
from pydantic import BaseModel, Extra, validator

from articat.config import ArticatConfig, ArticatMode, ConfigMixin
from articat.utils.datetime_utils import convert_to_datetime

if typing.TYPE_CHECKING:
    import pyspark

    from articat.catalog import Catalog

logger = logging.getLogger(__name__)


class Metadata(BaseModel):
    spark_schema: Optional[str] = None
    """String representation of spark schema"""
    schema_fields: Optional[list[str]] = None
    """List of fields in the schema"""
    arbitrary: dict[str, Any] = {}
    """
    Any arbitrary metadata of your choice, can be embedded dicts, lists etc.
    Valid types: https://cloud.google.com/appengine/docs/standard/python/datastore/entities#Properties_and_value_types
    """
    description: Optional[str] = None
    """Description"""

    def add_spark_df_info(self, df: pyspark.sql.DataFrame) -> Metadata:
        """Add information from Spark Dataframe"""
        if self.arbitrary is not None:
            self.arbitrary.update({"nrow": df.count()})
        else:
            self.arbitrary = {"nrow": df.count()}
        self.schema_fields = df.columns
        self.spark_schema = df._jdf.schema().treeString()
        return self


class Arbitrary(BaseModel):
    """
    This is a documentation model, arbitrary dict isn't guaranteed to
    have any specific structure, but having these fields that we **try**
    to populate by default in one place is useful.
    """

    nrow: Optional[int]
    git_repo_url: Optional[str]
    git_head_hash: Optional[str]
    call_site_lineno: Optional[int]
    step_relfname: Optional[str]
    task_relfname: Optional[str]
    bq_stage_load_job_id: Optional[str]
    """BQ job id used for staging BQ data"""
    bq_job_id: Optional[str]
    """BQ job id used to produce the final BQ data"""
    execution_url: Optional[str]
    """URL to the execution that created this Artifact (could be for example GitHub Action)"""

    def get_update_dict(self) -> dict[str, Union[int, str]]:
        """
        Returns dict with only the explicitly set fields.

        Useful to update arbitrary dict like this:

        a.metadata.arbitrary.update(Arbitrary(nrow=3).get_update_dict())

        ^ will only update the nrow field within arbitrary.
        """
        d = self.dict(exclude_unset=True)
        assert isinstance(d, dict)
        return d


# NOTE: see documentation below for the meaning of these fields
ID = str
Version = str
# NOTE: since `datetime` is also `date`, it's important that in
#       the union `datetime` precedes `date`.
Partition = Union[datetime, date]
EXECUTION_URL_ENV_NAME = "ARTICAT_EXECUTION_URL"
"""
If this env variable is set, the value will be recorded inside the arbitrary
dict. This could be for example URL to the GitHub Action workflow.
"""
T = TypeVar("T", bound="Artifact")


class Artifact(ConfigMixin, BaseModel):
    """Represents a single instance of a data Artifact"""

    class Config:
        """Core Artifact pydantic config"""

        extra = Extra.allow
        """
        Allow for extra properties to propagate into Artifact.

        This allows to parse any specialized Artifact as Artifact
        model, without "losing" extra properties.
        """
        underscore_attrs_are_private = True
        """Underscore attributes are private"""

    id: ID
    """Artifact ID, globally unique"""
    partition: Optional[Partition] = None
    """Artifact partition, akin to datetime "git commit hash" for the artifact"""
    metadata: Metadata = Metadata()
    """Partition's metadata"""
    version: Optional[Version] = None
    """Artifact version, akin to "git tag" for the artifact"""
    created: Optional[datetime] = None
    """Creation time"""
    _retire_entity: Optional[Mapping[str, Any]] = None
    # Note: this field is used to carry retired entity, it's not serialized
    _partition_str_format: ClassVar[str] = "%Y%m%dT%H%M%S"
    # string format for partition used in paths etc
    _config: Union[ArticatConfig, type[ArticatConfig]] = ArticatConfig

    @validator("partition")
    def partition_must_be_datetime(cls, v: Optional[Partition]) -> Optional[datetime]:
        return None if v is None else convert_to_datetime(v)

    def open_browser(self) -> None:
        """Opens browser with the URL associated with this artifact. Best effort."""
        import webbrowser

        webbrowser.open(self.browser_url())

    def browser_url(self) -> str:
        """Returns URL of the resource associated with this artifact"""
        raise NotImplementedError()

    def build(self) -> Artifact:
        """
        This method takes all the information provided in the current artifact
        and might decide to add more, for example for file based artifact,
        add file listing information, check if files exist, check size etc.
        """
        raise NotImplementedError(
            "You are trying to build abstract Artifact, "
            "use one of the concrete Artifact classes"
        )

    def _catalog(self) -> Type[Catalog]:
        return self.config().catalog()

    def _exclude_private_fields(self) -> set[str]:
        # TODO: remove since the config is in place
        return {f for f in self.__dict__ if f.startswith("_")}

    def _test_artifact_serialization(self) -> None:
        e = datastore.Entity()
        e.update(self.dict(exclude=self._exclude_private_fields()))
        entity_to_protobuf(e)

    def is_dev(self) -> bool:
        """Returns True if this artifact is in dev mode, False otherwise"""
        return self._is_dev_mode(self.id)

    @staticmethod
    def _is_dev_mode(id: Optional[ID]) -> bool:
        if not id:
            raise ValueError("id must be set")
        return id.startswith("_dev_")

    @staticmethod
    def _enforce_dev_mode(id: Optional[ID], dev: bool) -> Optional[str]:
        if id is None:
            return None
        if dev and not Artifact._is_dev_mode(id):
            return f"_dev_{id}"
        else:
            # NOTE: in the future we might log a warning here to use
            #       dev flag instead of id to specify the dev mode
            return id

    def __add_git_info(self) -> None:
        try:
            from articat.utils.utils import get_repo_and_hash

            repo_url, hash = get_repo_and_hash()
        except Exception as exc:
            logger.warning(f"Could not populate git info due to: {exc}")
            hash, repo_url = "UNKNOWN", "UNKNOWN"

        if hash.endswith("DIRTY") and not self.is_dev():
            # TODO (rav): make this an error eventually
            logger.warning(
                "Your git HEAD tree is dirty and you try to produce a production"
                " artifact, in the future this will raise an error!"
            )
        self.metadata.arbitrary.update(
            Arbitrary(git_repo_url=repo_url, git_head_hash=hash).get_update_dict()
        )

    def __add_exe_info(self) -> None:
        exe_url = os.environ.get(EXECUTION_URL_ENV_NAME)
        if exe_url is not None:
            self.metadata.arbitrary.update(
                Arbitrary(execution_url=exe_url.strip()).get_update_dict()
            )

    @classmethod
    def partitioned(
        cls: type[T],
        id: ID,
        partition: Optional[Partition] = None,
        *,
        dev: bool = False,
        config: Optional[ArticatConfig] = None,
    ) -> T:
        """
        CTOR for a partitioned Artifact.

        :param id: Artifact ID.
        :param partition: Artifact partition.
        :param dev: mode mode flag.
        :param config: optional custom config for this Artifact.
        """
        a = cls(id=Artifact._enforce_dev_mode(id, dev), partition=partition)
        if config is not None:
            a._config = config
        return a

    @classmethod
    def versioned(
        cls: type[T],
        id: ID,
        version: Version,
        *,
        dev: bool = False,
        config: Optional[ArticatConfig] = None,
    ) -> T:
        """
        CTOR for a versioned Artifact.

        :param id: Artifact ID.
        :param version: Artifact version.
        :param dev: mode mode flag.
        :param config: optional custom config for this Artifact.
        """
        a = cls(id=Artifact._enforce_dev_mode(id, dev), version=version)
        if config is not None:
            a._config = config
        return a

    def spec(self) -> dict[str, Union[ID, Partition, Version]]:
        """
        Artifact spec is enough information to uniquely identify this
        artifact up to the partition/version. Useful to debug messages
        or recording dependencies.
        """
        r = self.dict(include={"id", "partition", "version"})
        assert isinstance(r, dict)
        return r

    def fetch(self: T, catalog: Optional[Type[Catalog]] = None) -> T:
        """
        When Artifact is used as a "spec", it doesn't have full information,
        use this method to fetch all the metadata from the Catalog.
        """
        assert isinstance(self, Artifact)

        catalog = catalog or self._catalog()
        return catalog.get(
            id=self.id,
            partition=self.partition,
            version=self.version,
            model=self.__class__,
        )

    def record_deps(self: T, *deps: Artifact) -> T:
        """Record artifact's dependencies. Users can record deps using multiple
        calls to record_deps to appends deps"""
        assert isinstance(self, Artifact)
        d = self.metadata.arbitrary.get("deps", [])
        # dep spec will include just {id, partition, version},
        # having {id, partition} would actually be enough since
        # versioned artifacts still have unique partition, but
        # for completeness let's include version
        dep_spec = [i.spec() for i in deps]
        self.metadata.arbitrary.update(dict(deps=d + dep_spec))
        return self

    def __enter__(self: T) -> T:
        # TODO (rav): this should be refactored and lean more on the config
        if (
            not hasattr(self, "__test__")
            and not self._is_dev_mode(self.id)
            and self.config().mode() != ArticatMode.local
            and self.config().mode() != ArticatMode.test
        ):
            if "ARTICAT_PROD" not in environ:
                raise ValueError(
                    f"Looks like {self.spec()} would go to the production catalog,"
                    " but your environment is missing ARTICAT_PROD, aborting. If you truly"
                    " intend to write to production, set environment variable ARTICAT_PROD=1."
                )

        if self.created is not None:
            raise ValueError("You should not set created field by yourself")
        self.created = datetime.utcnow()
        if self.partition is None:
            self.partition = datetime.utcnow().replace(microsecond=0)
        if self.metadata is None:
            self.metadata = Metadata()
        if self.metadata.arbitrary is None:
            self.metadata.arbitrary = {}

        if self.is_dev():
            logger.warning(
                f"{self.spec()} is in the development mode, your outputs get deleted after 30 days"
            )

        assert self.metadata is not None
        assert self.created is not None
        assert self.partition is not None
        assert self.metadata.arbitrary is not None

        self.__add_git_info()
        self.__add_exe_info()

        # Check if the artifact metadata already exists:
        try:
            # NOTE: we use protected _lookup because we want to parse raw object later
            if self.version is not None:
                # NOTE: if version is set, we check by version
                e = next(
                    iter(self._catalog()._lookup(id=self.id, version=self.version))
                )
            else:
                e = next(
                    iter(
                        self._catalog()._lookup(
                            id=self.id,
                            partition_dt_start=self.partition,
                            partition_dt_end=self.partition,
                        )
                    )
                )
            if self.is_dev():
                logger.warning(
                    f"Dev mode will overwrite metadata for previous {self.id}"
                )
                object.__setattr__(self, "_retire_entity", e)
                return self
            raise ValueError("Catalog already has an entry for this artifact!")
        except StopIteration:
            return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if not exc_type:
            self._catalog().save(self.build())
            logger.info(f"Artifact {self.spec()} materialized at: {self.browser_url()}")
        object.__setattr__(self, "_retire_entity", None)


NoneArtifact = typing.cast(Artifact, object())
"""
This is a hack to allow keyword arguments in derived classes of Step
and fulfill LSP. You can use this object to provide default values for
input Artifacts, that will be injected by the orchestrator. We might
build it up a bit more later on, if the we see too many cryptic errors.
"""
