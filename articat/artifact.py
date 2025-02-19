from __future__ import annotations

import logging
import os
import typing
from datetime import date, datetime
from os import environ
from types import TracebackType
from typing import Any, ClassVar, TypeVar

from google.cloud import datastore
from google.cloud.datastore.helpers import entity_to_protobuf
from pydantic.v1 import BaseModel, Extra, validator

from articat.config import ArticatConfig, ArticatMode, ConfigMixin
from articat.utils.datetime_utils import convert_to_datetime

if typing.TYPE_CHECKING:
    import pyspark

    from articat.catalog import Catalog

logger = logging.getLogger(__name__)


class ValueNotSupplied:
    """
    An instance of this type is used to indicate that a parameter was not supplied by the user,
    while still allowing None to be a valid value.
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> ValueNotSupplied:
        # NOTE: this is not thread safe
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return "NOT_SUPPLIED"

    def __bool__(self) -> bool:
        return False


not_supplied: None = ValueNotSupplied()  # type: ignore[assignment]
"""
Static typing is set to None, to make mypy happy across the codebase.
"""


class Metadata(BaseModel):
    spark_schema: str | None = None
    """String representation of spark schema"""
    schema_fields: list[str] | None = None
    """List of fields in the schema"""
    arbitrary: dict[str, Any] = {}
    """
    Any arbitrary metadata of your choice, can be embedded dicts, lists etc.
    Valid types: https://cloud.google.com/appengine/docs/standard/python/datastore/entities#Properties_and_value_types
    """
    description: str | None = None
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

    nrow: int | None
    git_repo_url: str | None
    git_head_hash: str | None
    call_site_lineno: int | None
    step_relfname: str | None
    task_relfname: str | None
    bq_stage_load_job_id: str | None
    """BQ job id used for staging BQ data"""
    bq_job_id: str | None
    """BQ job id used to produce the final BQ data"""
    execution_url: str | None
    """URL to the execution that created this Artifact (could be for example GitHub Action)"""

    def get_update_dict(self) -> dict[str, int | str]:
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
Partition = datetime | date
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
    partition: Partition | None = None
    """Artifact partition, akin to datetime "git commit hash" for the artifact"""
    metadata: Metadata = Metadata()
    """Partition's metadata"""
    version: Version | None = not_supplied
    """Artifact version, akin to "git tag" for the artifact"""
    created: datetime | None = None
    """Creation time"""
    _partition_str_format: ClassVar[str] = "%Y%m%dT%H%M%S"
    # string format for partition used in paths etc
    _config: ArticatConfig | type[ArticatConfig] = ArticatConfig

    @validator("partition")
    def partition_must_be_datetime(cls, v: Partition | None) -> datetime | None:
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

    def _catalog(self) -> type[Catalog]:
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
    def _is_dev_mode(id: ID | None) -> bool:
        if not id:
            raise ValueError("id must be set")
        return id.startswith("_dev_")

    @staticmethod
    def _enforce_dev_mode(id: ID | None, dev: bool) -> str | None:
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
        partition: Partition | None = None,
        *,
        dev: bool = False,
        config: ArticatConfig | None = None,
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
        config: ArticatConfig | None = None,
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

    def spec(self) -> dict[str, ID | Partition | Version]:
        """
        Artifact spec is enough information to uniquely identify this
        artifact up to the partition/version. Useful to debug messages
        or recording dependencies.
        """
        r = self.dict(include={"id", "partition", "version"})
        assert isinstance(r, dict)
        return r

    def fetch(self: T, catalog: type[Catalog] | None = None) -> T:
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
        self.metadata.arbitrary.update({"deps": d + dep_spec})
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

        if self.is_dev():
            return self
        else:
            # Check if the artifact metadata already exists:
            try:
                next(
                    iter(
                        self._catalog().lookup(
                            id=self.id,
                            version=self.version,
                            partition_dt_start=self.partition,
                            partition_dt_end=self.partition,
                        )
                    )
                )
                raise ValueError(f"Artifact already exists, spec: {self.spec()}")
            except StopIteration:
                return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if not exc_type:
            self._catalog().save(self.build())
            logger.info(f"Artifact {self.spec()} materialized at: {self.browser_url()}")

    def deprecate(self) -> None:
        """
        Deprecate this artifact. This is a metadata operation, the physical data will NOT be touched.
        After an artifact is deprecated it won't be visible in the catalog anymore. The physical data
        will be cleaned up by a separate process and is an implementation detail of the catalog.
        """
        assert self.id is not None
        assert (self.version not in {None, not_supplied}) or (
            self.partition is not None
        )
        artifact = self.fetch()
        self._catalog().deprecate(artifact)

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Override the default dict() method to handle the case where version is not supplied.
        """
        d = super().dict(*args, **kwargs)
        if d["version"] is not_supplied:
            d["version"] = None
        return d

    def json(self, *args: Any, **kwargs: Any) -> str:
        """
        Override the default json() method to handle the case where version is not supplied.
        """
        if self.version is not_supplied:
            return self.copy(update={"version": None}).json(*args, **kwargs)
        return super().json(*args, **kwargs)


NoneArtifact = typing.cast(Artifact, object())
"""
This is a hack to allow keyword arguments in derived classes of Step
and fulfill LSP. You can use this object to provide default values for
input Artifacts, that will be injected by the orchestrator. We might
build it up a bit more later on, if the we see too many cryptic errors.
"""
