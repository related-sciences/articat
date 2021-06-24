class ArticatConfig:
    # TODO: temp workaround for the open-sourcing step
    gcp_project: str = "UNSET"
    bq_prod_dataset: str = "UNSET"
    bq_dev_dataset: str = "UNSET"
    gs_tmp_bucket: str = "UNSET"
    gs_dev_bucket: str = "UNSET"
    gs_prod_bucket: str = "UNSET"
