from __future__ import annotations

import dbm
from datetime import datetime
from hashlib import md5
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from articat.artifact import ID, Artifact, Metadata, Partition, Version
from articat.catalog import Catalog
from articat.utils.datetime_utils import convert_to_datetime


class CatalogLocal(Catalog):
    """
    This is local flavour of the Catalog. This implementation is purely for
    exploration, test, presentation etc. This implementation is inefficient
    by design. Do not use for production use cases. This implementation is
    not thread-safe.
    """

    @classmethod
    def _get_db(cls) -> dbm._Database:
        # NOTE: this is inefficient, but that's fine for the purpose of the local mode
        Path(cls.config().local_db_dir()).mkdir(parents=True, exist_ok=True)
        return dbm.open(
            Path(cls.config().local_db_dir()).joinpath("store").as_posix(), flag="c"
        )

    @classmethod
    def _compute_key(cls, artifact: Artifact) -> str:
        h = md5()
        h.update(artifact.id.encode())
        if artifact.version:
            h.update(artifact.version.encode())
        if artifact.partition:
            assert isinstance(artifact.partition, datetime)
            h.update(artifact.partition.isoformat().encode())
        return h.hexdigest()

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
    ) -> Iterable[Mapping[str, Any]]:
        if metadata is not None:
            raise ValueError("Local mode doesn't currently support lookup by metadata")
        result = []
        with cls._get_db() as db:
            for k in db.keys():
                a = Artifact.parse_raw(db[k])
                if id is not None:
                    if a.id != id:
                        continue
                if version is not None:
                    if a.version != version:
                        continue
                if (
                    partition_dt_start is not None or partition_dt_end is not None
                ) and a.partition is not None:
                    if partition_dt_start == partition_dt_end and partition_dt_start:
                        if a.partition != convert_to_datetime(partition_dt_start):
                            continue
                    else:
                        if partition_dt_start is not None:
                            if a.partition < convert_to_datetime(partition_dt_start):
                                continue
                        if partition_dt_end is not None:
                            if a.partition >= convert_to_datetime(partition_dt_end):
                                continue
                result.append(a.dict())
        yield from sorted(
            result, key=lambda x: (x["partition"], x["version"]), reverse=True
        )

    @classmethod
    def save(cls, artifact: Artifact) -> Artifact:
        with cls._get_db() as db:
            if artifact._retire_entity is not None:
                del db[cls._compute_key(Artifact.parse_obj(artifact._retire_entity))]
            db[cls._compute_key(artifact)] = artifact.json()
        return artifact
