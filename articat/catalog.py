from __future__ import annotations

import logging
from datetime import date, datetime
from functools import lru_cache
from typing import Any, Iterable, Optional, Type, TypeVar, Union, overload

from google.cloud import datastore
from google.cloud.datastore import Client, Entity, Key

from articat.artifact import ID, Artifact, Metadata, Partition, Version
from articat.config import ArticatConfig, ConfigMixin
from articat.fs_artifact import FSArtifact

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="Artifact")


class Catalog(ConfigMixin):
    """RS Data Catalog"""

    _config: Union[Type[ArticatConfig], ArticatConfig] = ArticatConfig

    @classmethod
    @lru_cache
    def _client(
        cls, project: Optional[str] = None, namespace: Optional[str] = None
    ) -> datastore.Client:
        project = project or cls.config.gcp_project
        return datastore.Client(project=project, namespace=namespace)

    @classmethod
    def _dev_client(cls, project: Optional[str] = None) -> datastore.Client:
        project = project or cls.config.gcp_project
        return cls._client(project=project, namespace="dev")

    @classmethod
    def _retired_client(cls, project: Optional[str] = None) -> datastore.Client:
        project = project or cls.config.gcp_project
        return cls._client(project=project, namespace="retired")

    @classmethod
    @overload
    def get(
        cls, id: ID, *, version: Optional[Version], dev: bool = False
    ) -> FSArtifact:
        ...

    @classmethod
    @overload
    def get(
        cls, id: ID, *, partition: Optional[Partition], dev: bool = False
    ) -> FSArtifact:
        ...

    @classmethod
    @overload
    def get(cls, id: ID, *, dev: bool = False) -> FSArtifact:
        ...

    @classmethod
    @overload
    def get(
        cls,
        id: ID,
        *,
        partition: Optional[Partition] = None,
        version: Optional[Version] = None,
        model: Type[T],
        dev: bool = False,
    ) -> T:
        ...

    @classmethod
    def get(
        cls,
        id: ID,
        *,
        version: Optional[Version] = None,
        partition: Optional[Partition] = None,
        model: Type[T] = FSArtifact,
        dev: bool = False,
    ) -> T:
        """
        Get a data artifact given its id. You should specify partition or
        version for deterministic retrieval. Model class can be specified
        to parse the result as a specific model.

        If you do not specify partition, you will get the latest partition,
        which might not be the latest created artifact with that ID. If
        you want to fetch the latest artifact with a given ID, sort by
        created field, example:

        ```
        sorted(Catalog.lookup("foo"), key=lambda x: x.created)[-1]
        ```

        :param id: artifact id
        :param version: optional artifact version
        :param partition: optional datetime/date partition
        :param model: optional model class (e.g. FSArtifact)
        :type dev: whether to use dev mode (default False)
        """
        if not partition and not version:
            a = cls.latest_partition(id, model=model, dev=dev)
            assert isinstance(a, Artifact)
            logger.warning(
                "No version or partition specified, fetching latest partition. "
                "This should be avoided in non development pipelines. "
                f"Fetched {a.id}, partition: {a.partition} version: {a.version}"
            )
            return a
        try:
            return next(
                iter(
                    cls.lookup(
                        id=id,
                        partition_dt_start=partition,
                        partition_dt_end=partition,
                        version=version,
                        limit=1,
                        model=model,
                        dev=dev,
                    )
                )
            )
        except StopIteration as e:
            req = dict(id=id, partition=partition, version=version, dev=dev)
            raise ValueError(f"Can't find requested artifact {req}") from e

    @staticmethod
    def _convert_to_datetime(dt: Partition) -> datetime:
        # NOTE: isinstance(date.today(), datetime) is True
        return (
            datetime.combine(dt, datetime.min.time())
            if not isinstance(dt, datetime)
            else dt
        )

    @classmethod
    @overload
    def latest_partition(cls, id: ID, *, dev: bool = False) -> FSArtifact:
        ...

    @classmethod
    @overload
    def latest_partition(
        cls, id: ID, *, model: Type[T] = FSArtifact, dev: bool = False
    ) -> T:
        ...

    @classmethod
    def latest_partition(
        cls, id: ID, *, model: Type[T] = FSArtifact, dev: bool = False
    ) -> T:
        try:
            return next(
                iter(
                    cls.lookup(
                        id=id,
                        partition_dt_start=date.min,
                        limit=1,
                        model=model,
                        dev=dev,
                    )
                )
            )
        except StopIteration as e:
            raise ValueError(f"Can't find requested artifact {id}") from e

    @classmethod
    def lookup(
        cls,
        id: Optional[ID] = None,
        partition_dt_start: Optional[Partition] = None,
        partition_dt_end: Optional[Partition] = None,
        version: Optional[Version] = None,
        metadata: Optional[Metadata] = None,
        limit: Optional[int] = None,
        model: Type[T] = FSArtifact,
        dev: bool = False,
    ) -> Iterable[Artifact]:
        """
        Lookup/search atop Catalog. You can search based on id, partitions, version
        and most metadata. Keep in mind that substring matches, case-insensitive matches,
        or so-called full-text search are not supported right, if that is your
        use case match as much as you can and do further matching locally.
        You can't lookup by id + partition/version and metadata at the same time.

        Examples:

        ```
        Catalog.lookup() # returns an iterable over whole catalog
        # returns an iterable over all partitions/versions within my-id:
        Catalog.lookup("my-id")
        # return an iterable over partitions since yesterday, ordered desc by partition
        # aka latest is first in the iterable:
        Catalog.lookup("my-id", partition_dt_start=yesterday)
        # lookup my-id with version 0.1.2
        Catalog.lookup("my-id", version="0.1.2")
        ```

        :param id: Id of the artifact/dataset group
        :param partition_dt_start: start datetime/date partition (including)
        :param partition_dt_end: end datetime/date partition (excluding)
        :param version: version to match
        :param metadata: metadata to match
        :param limit: limit number of results
        :param model: what model to use to parse the results
        :param dev: whether to use dev mode
        :return: Iterable[Artifact]
        """
        yield from (
            model.parse_obj(r)
            for r in cls._lookup(
                id=id,
                partition_dt_start=partition_dt_start,
                partition_dt_end=partition_dt_end,
                version=version,
                metadata=metadata,
                limit=limit,
                dev=dev,
            )
        )

    @classmethod
    def _lookup(
        cls,
        client: Optional[Client] = None,
        id: Optional[ID] = None,
        partition_dt_start: Optional[Partition] = None,
        partition_dt_end: Optional[Partition] = None,
        version: Optional[Version] = None,
        metadata: Optional[Metadata] = None,
        limit: Optional[int] = None,
        dev: bool = False,
    ) -> Iterable[Entity]:
        id = Artifact._enforce_dev_mode(id, dev)
        if id and Artifact._is_dev_mode(id):
            msg_spec = dict(
                id=id,
                partition_dt_start=partition_dt_start,
                partition_dt_end=partition_dt_end,
                version=version,
            )
            logger.warning(
                f"Looking up dev artifact {msg_spec}, dev data gets deleted after 30 days"
            )
            client = client or cls._dev_client()
        elif id is None and dev:
            client = client or cls._dev_client()
        else:
            client = client or cls._client()
        logger.debug(
            f"Lookup of {id}, partition: [{partition_dt_start}, {partition_dt_end}), version: {version}, "
            f"metadata: {metadata}, limit: {limit}"
        )
        if metadata is not None:
            if metadata.spark_schema:
                raise ValueError(
                    "Spark schema lookup not supported, use schema_fields instead"
                )
            if metadata.description:
                raise ValueError("Description lookup not supported")
            if metadata.arbitrary:
                raise ValueError("Arbitrary lookup not supported")
        if metadata is not None and any(
            x is not None for x in [partition_dt_end, partition_dt_start]
        ):
            raise ValueError("Can't lookup based on both metadata and partition")
        if id:
            query = client.query(kind="Partition", ancestor=client.key("Artifact", id))
        else:
            query = client.query(kind="Partition")
        if partition_dt_start is not None or partition_dt_end is not None:
            if (
                partition_dt_start
                and partition_dt_end
                and partition_dt_start == partition_dt_end
            ):
                query.add_filter(
                    "partition", "=", cls._convert_to_datetime(partition_dt_start)
                )
            else:
                if partition_dt_start is not None:
                    query.add_filter(
                        "partition", ">=", cls._convert_to_datetime(partition_dt_start)
                    )
                if partition_dt_end is not None:
                    query.add_filter(
                        "partition", "<", cls._convert_to_datetime(partition_dt_end)
                    )
                query.order = ["-partition"]
        if version:
            query.add_filter("version", "=", version)
        if metadata is not None:
            if metadata.schema_fields:
                for f in metadata.schema_fields:
                    query.add_filter("metadata.schema_fields", "=", f)
            if metadata.arbitrary is not None:
                for k, v in metadata.arbitrary.items():
                    query.add_filter(f"metadata.arbitrary.{k}", "=", v)
        yield from query.fetch(limit)

    @classmethod
    def _put_entity(cls, key: Key, artifact: Artifact, client: Client) -> None:
        catalog_entity = datastore.Entity(key)
        record = artifact.dict(exclude=artifact._exclude_private_fields())

        if artifact.metadata is not None:
            # This fields are not indexed, and therefor can't be match on
            # Note: due to https://github.com/googleapis/google-cloud-python/issues/1206
            metadata_entity = datastore.Entity(
                exclude_from_indexes=("spark_schema", "description", "arbitrary")
            )
            metadata_entity.update(artifact.metadata.dict())

            # Do not index arbitrary dict:
            arbitrary_entity = datastore.Entity(
                exclude_from_indexes=tuple(artifact.metadata.arbitrary.keys())
            )
            arbitrary_entity.update(artifact.metadata.arbitrary)
            metadata_entity["arbitrary"] = arbitrary_entity
            record["metadata"] = metadata_entity

        catalog_entity.update(record)
        client.put(catalog_entity)

    @classmethod
    def save(cls, artifact: Artifact) -> Artifact:
        """
        Saves a fully formed artifact into the Catalog. In most cases you
        should use artifact with a with-statement context, which builds
        and saves the artifact for you.
        :param artifact: artifact to save
        :return: saved Artifact
        """
        if not artifact.id:
            raise ValueError("id must be set to save an artifact")
        if artifact.partition is None:
            raise ValueError("partition must be set to save an artifact")
        if artifact.is_dev():
            client = cls._dev_client()
        else:
            client = cls._client()
        if artifact._retire_entity is not None:
            # We don't delete any metadata we copy it to the retired namespace
            r_id, r_art = artifact._retire_entity
            retired_client = cls._retired_client()
            retired_key = retired_client.key("Artifact", artifact.id, "Partition", r_id)
            cls._put_entity(retired_key, r_art, retired_client)
            key = client.key("Artifact", artifact.id, "Partition", r_id)
        else:
            key = client.key("Artifact", artifact.id, "Partition")
        # TODO (rav): make this a transaction at the key level
        cls._put_entity(key, artifact, client)
        return artifact

    @classmethod
    def to_dataframe(
        cls,
        dev: bool = False,
        limit: Optional[int] = None,
        include_arbitrary: bool = True,
    ) -> Any:
        """
        Return DataFrame representation of the Catalog.

        Use `dev=True` to get the development view of the Catalog. Use `limit` to reduce the
        size of the output. `include_arbitrary=False` to avoid expanding arbitrary field.
        """
        try:
            import pandas as pd
        except ImportError:
            logger.exception("to_dataframe requires pandas, install pandas ...")
            raise

        exclude = None if include_arbitrary else dict(metadata=dict(arbitrary=...))
        catalog_dicts = list(
            i.dict(exclude=exclude)
            for i in cls.lookup(dev=dev, limit=limit, model=Artifact)
        )
        return pd.json_normalize(catalog_dicts, sep="_")
