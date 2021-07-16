from __future__ import annotations

import logging
from functools import lru_cache
from typing import Iterable, Optional, TypeVar

from google.cloud import datastore
from google.cloud.datastore import Client, Entity, Key

from articat.artifact import ID, Artifact, Metadata, Partition, Version
from articat.catalog import Catalog
from articat.utils.datetime_utils import convert_to_datetime

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Artifact)


class CatalogDatastore(Catalog):
    """GCP Datastore Articat Catalog"""

    @classmethod
    @lru_cache
    def _client(
        cls, project: Optional[str] = None, namespace: Optional[str] = None
    ) -> datastore.Client:
        project = project or cls.config().gcp_project()
        return datastore.Client(project=project, namespace=namespace)

    @classmethod
    def _dev_client(cls, project: Optional[str] = None) -> datastore.Client:
        project = project or cls.config().gcp_project()
        return cls._client(project=project, namespace="dev")

    @classmethod
    def _retired_client(cls, project: Optional[str] = None) -> datastore.Client:
        project = project or cls.config().gcp_project()
        return cls._client(project=project, namespace="retired")

    @classmethod
    def _lookup(
        cls,
        id: Optional[ID] = None,
        partition_dt_start: Optional[Partition] = None,
        partition_dt_end: Optional[Partition] = None,
        version: Optional[Version] = None,
        metadata: Optional[Metadata] = None,
        limit: Optional[int] = None,
        dev: bool = False,
        client: Optional[Client] = None,
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
                    "partition", "=", convert_to_datetime(partition_dt_start)
                )
            else:
                if partition_dt_start is not None:
                    query.add_filter(
                        "partition", ">=", convert_to_datetime(partition_dt_start)
                    )
                if partition_dt_end is not None:
                    query.add_filter(
                        "partition", "<", convert_to_datetime(partition_dt_end)
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
            re = artifact._retire_entity
            assert isinstance(re, Entity)
            r_art = artifact.__class__.parse_obj(re)
            retired_client = cls._retired_client()
            retired_key = retired_client.key(
                "Artifact", artifact.id, "Partition", re.id
            )
            cls._put_entity(retired_key, r_art, retired_client)
            key = client.key("Artifact", artifact.id, "Partition", re.id)
        else:
            key = client.key("Artifact", artifact.id, "Partition")
        # TODO (rav): make this a transaction at the key level
        cls._put_entity(key, artifact, client)
        return artifact
