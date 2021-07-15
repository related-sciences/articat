from __future__ import annotations

import logging
from datetime import date
from typing import Any, Iterable, Mapping, Optional, Type, TypeVar, Union, overload

from articat.artifact import ID, Artifact, Metadata, Partition, Version
from articat.config import ArticatConfig, ConfigMixin

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Artifact)


class Catalog(ConfigMixin):
    """RS Data Catalog"""

    _config: Union[Type[ArticatConfig], ArticatConfig] = ArticatConfig

    @classmethod
    @overload
    def get(
        cls,
        id: ID,
        *,
        version: Optional[Version],
        dev: bool = False,
    ) -> Artifact:
        ...

    @classmethod
    @overload
    def get(
        cls,
        id: ID,
        *,
        partition: Optional[Partition],
        dev: bool = False,
    ) -> Artifact:
        ...

    @classmethod
    @overload
    def get(cls, id: ID, *, dev: bool = False) -> Artifact:
        ...

    @classmethod
    @overload
    def get(cls, id: ID, *, model: Type[T], dev: bool = False) -> T:
        ...

    @classmethod
    @overload
    def get(
        cls,
        id: ID,
        *,
        model: Type[T],
        version: Optional[Version] = None,
        partition: Optional[Partition] = None,
        dev: bool = False,
    ) -> T:
        ...

    # NOTE: no return type due to: https://github.com/python/mypy/issues/3737
    @classmethod
    def get(  # type: ignore[no-untyped-def]
        cls,
        id: ID,
        *,
        version: Optional[Version] = None,
        partition: Optional[Partition] = None,
        model: Type[Artifact] = Artifact,
        dev: bool = False,
    ):
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

    @classmethod
    @overload
    def latest_partition(cls, id: ID, *, dev: bool = False) -> Artifact:
        ...

    @classmethod
    @overload
    def latest_partition(cls, id: ID, *, model: Type[T], dev: bool = False) -> T:
        ...

    # NOTE: no return type due to: https://github.com/python/mypy/issues/3737
    @classmethod
    def latest_partition(  # type: ignore[no-untyped-def]
        cls, id: ID, *, model: Type[Artifact] = Artifact, dev: bool = False
    ):
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
    @overload
    def lookup(
        cls,
        id: Optional[ID] = None,
        *,
        partition_dt_start: Optional[Partition] = None,
        partition_dt_end: Optional[Partition] = None,
        version: Optional[Version] = None,
        metadata: Optional[Metadata] = None,
        limit: Optional[int] = None,
        dev: bool = False,
    ) -> Iterable[Artifact]:
        ...

    @classmethod
    @overload
    def lookup(
        cls,
        id: Optional[ID] = None,
        *,
        model: Type[T],
        partition_dt_start: Optional[Partition] = None,
        partition_dt_end: Optional[Partition] = None,
        version: Optional[Version] = None,
        metadata: Optional[Metadata] = None,
        limit: Optional[int] = None,
        dev: bool = False,
    ) -> Iterable[T]:
        ...

    # NOTE: no return type due to: https://github.com/python/mypy/issues/3737
    @classmethod
    def lookup(  # type: ignore[no-untyped-def]
        cls,
        id: Optional[ID] = None,
        *,
        partition_dt_start: Optional[Partition] = None,
        partition_dt_end: Optional[Partition] = None,
        version: Optional[Version] = None,
        metadata: Optional[Metadata] = None,
        limit: Optional[int] = None,
        model: Type[Artifact] = Artifact,
        dev: bool = False,
    ):
        """
        Lookup/search atop Catalog. You can search based on id, partitions, version
        and most metadata. Keep in mind that substring matches, case-insensitive matches,
        or so-called full-text search are not supported, if that is your
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
        :return: Iterable[<model>]
        """
        assert issubclass(model, Artifact)
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
        id: Optional[ID] = None,
        partition_dt_start: Optional[Partition] = None,
        partition_dt_end: Optional[Partition] = None,
        version: Optional[Version] = None,
        metadata: Optional[Metadata] = None,
        limit: Optional[int] = None,
        dev: bool = False,
    ) -> Iterable[Mapping[str, Any]]:
        raise NotImplementedError()

    @classmethod
    def save(cls, artifact: Artifact) -> Artifact:
        """
        Saves a fully formed artifact into the Catalog. In most cases you
        should use artifact with a with-statement context, which builds
        and saves the artifact for you.
        :param artifact: artifact to save
        :return: saved Artifact
        """
        raise NotImplementedError()

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
            i.dict(exclude=exclude)  # type: ignore[arg-type]
            for i in cls.lookup(dev=dev, limit=limit, model=Artifact)
        )
        return pd.json_normalize(catalog_dicts, sep="_")
