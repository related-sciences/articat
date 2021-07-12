import logging
import uuid
from datetime import date, timedelta
from functools import lru_cache
from types import TracebackType
from typing import Any, ClassVar, Optional, Type

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from articat.artifact import ID, Arbitrary, Artifact, Partition, Version
from articat.config import ArticatConfig

logger = logging.getLogger(__name__)


class BQArtifact(Artifact):
    """
    BigQuery artifact for the RS Catalog. It creates natively partitioned table in BigQuery.
    Partitioned table doc: https://cloud.google.com/bigquery/docs/partitioned-tables#ingestion_time.
    The partition filter is required on the queries.

    This artifact is currently in "beta", we want to figure out how do we want to
    use BigQuery and what works best.
    """

    _bq_partition_date_format: ClassVar[str] = "%Y%m%d"
    """BQ uses this format to specify the day partitioning table"""
    _staging_table: Optional[str] = None
    """Staging table the user should write data to"""
    table_id: str = ""
    """
    BQ table id, this exists when you retrieve the artifact and right
    after the write. Format: <PROJECT_ID>.<DATASET_ID>.<TABLE_ID>.
    """

    @staticmethod
    @lru_cache
    def bq_client(project: Optional[str] = None, **kwargs: Any) -> bigquery.Client:
        """MVP BQ client for BQ Artifacts"""
        # TODO (rav): add support for custom GCP project/etc
        return bigquery.Client(project=project, **kwargs)

    @classmethod
    def versioned(
        cls,
        id: ID,
        version: Version,
        *,
        dev: bool = False,
        config: Optional[ArticatConfig] = None,
    ) -> "BQArtifact":
        """Versioned BQ artifacts are not supported"""
        raise NotImplementedError("Versioned BigQuery Artifact is not supported")

    @classmethod
    def partitioned(
        cls,
        id: ID,
        partition: Optional[Partition] = date.today(),
        *,
        dev: bool = False,
        config: Optional[ArticatConfig] = None,
    ) -> "BQArtifact":
        """Partitioned BQ artifact, the partition must be a date"""
        assert partition is not None
        # TODO (rav): add support for up to an hour resolution
        if partition.resolution != timedelta(days=1):
            raise ValueError("Partition resolution for BQ artifact must be a day")
        return (
            super()
            .partitioned(id=id, partition=partition, dev=dev, config=config)
            ._best_effort_tag_with_call_site()
        )

    def __enter__(self) -> "BQArtifact":
        r: BQArtifact = super().__enter__()
        assert r.partition

        if not r.created:
            raise ValueError("created must be set")
        if not r.id:
            raise ValueError("ID must be set")

        partition_str = r.created.strftime(Artifact._partition_str_format)
        # NOTE: this style of assignment is required: https://github.com/samuelcolvin/pydantic/issues/655
        object.__setattr__(
            r,
            "_staging_table",
            f"{r.config.gcp_project}.{r.config.fs_tmp_prefix}.{r.id}_{partition_str}_{uuid.uuid4()}",
        )

        partition_str = r.partition.strftime(BQArtifact._bq_partition_date_format)
        if r.is_dev():
            r.table_id = f"{r.config.gcp_project}.{r.config.bq_dev_dataset}.{r.id}${partition_str}"
        else:
            logger.warning(
                "Production BigQuery artifacts are not supported yet, data will be saved to "
                f"the development dataset: {r.config.bq_dev_dataset}."
            )
            # r.table_id = f"{r.config.gcp_project}.{r._dataset}.{r.id}${partition_str}"
            r.table_id = f"{r.config.gcp_project}.{r.config.bq_dev_dataset}.{r.id}${partition_str}"
        logger.debug(f"Final data of {r.spec()} will end up in {r.table_id}")
        return r

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        super().__exit__(exc_type, exc_val, exc_tb)
        # NOTE: this style of set is required: https://github.com/samuelcolvin/pydantic/issues/655
        object.__setattr__(self, "_staging_table", None)

    def build(self) -> "BQArtifact":
        assert self._staging_table, "Staging table must be set"
        assert self.table_id, "Destination table id must be set"
        assert self.partition, "Partition must be set"

        bq_client = self.bq_client(self.config.gcp_project)

        staged_table = bq_client.get_table(self._staging_table)

        try:
            tbl = bq_client.get_table(self.table_id.split("$")[0])
            if (
                tbl.description != self.metadata.description
                or tbl.schema != staged_table.schema
            ):
                # TODO: update only what is actually needed
                tbl.description = self.metadata.description
                tbl.schema = staged_table.schema
                bq_client.update_table(table=tbl, fields=["schema", "description"])
            logger.debug(f"Found table {tbl}")
        except NotFound:
            logger.info(
                f"Table {self.table_id} does not exist, will create a new partitioned table"
            )
            tbl = bigquery.Table(
                self.table_id.split("$")[0],
                schema=bq_client.get_table(self._staging_table).schema,
            )
            tbl.description = self.metadata.description
            tbl.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                # TODO (rav): what should be the default partition expiration, if not
                #             specified it will inherit the dataset expiration
                # expiration_ms=...,
            )
            tbl.require_partition_filter = True
            bq_client.create_table(tbl, exists_ok=False)

        if self.is_dev():
            write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
        else:
            write_disposition = bigquery.job.WriteDisposition.WRITE_EMPTY

        job_config = bigquery.CopyJobConfig(
            create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=write_disposition,
        )

        cp_job = bq_client.copy_table(
            sources=self._staging_table,
            destination=self.table_id,
            job_config=job_config,
        )
        cp_job_result = cp_job.result()

        destination_table = bq_client.get_table(self.table_id)
        logger.info(f"Loaded {destination_table.num_rows} rows to {self.table_id}")
        self.metadata.arbitrary.update(
            Arbitrary(bq_job_id=cp_job_result.job_id).get_update_dict()
        )
        return self

    def browser_url(self) -> str:
        project_id, dataset_id, table_id = self.table_id.split(".")
        bq_url_root = "https://console.cloud.google.com/bigquery"
        return f"{bq_url_root}?project={project_id}&p={project_id}&d={dataset_id}&page=table&t={table_id}"
