import logging
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict

import pytest
from _pytest.config import Config
from _pytest.config.argparsing import Parser

from articat.config import ArticatConfig, ArticatMode

logger = logging.getLogger(__name__)


def pytest_addoption(parser: Parser) -> None:
    parser.addoption(
        "--datastore_emulated",
        action="store_true",
        default=False,
        help="enables datastore tests",
    )


def pytest_configure(config: Config) -> None:
    config.addinivalue_line(
        "markers", "datastore_emulated: test requires datastore emulator"
    )
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    run_datastore = config.getoption("--datastore_emulated")
    skip_datastore = pytest.mark.skip(
        reason="need datastore emulator (--datastore_emulated)"
    )
    for item in items:
        if "datastore_emulated" in item.keywords:
            if not run_datastore:
                item.add_marker(skip_datastore)


@pytest.fixture(scope="function")
def uid() -> str:
    return uuid.uuid4().hex


@pytest.fixture(scope="function")
def tmppath(tmpdir):
    return Path(tmpdir)


def get_test_config() -> Dict[str, Dict[str, Any]]:
    test_fs_prefix = Path(tempfile.mkdtemp())
    logger.info(f"Test fs prefix: {test_fs_prefix.as_posix()}")
    for d in ("tmp", "dev", "prod"):
        test_fs_prefix.joinpath(d).mkdir(parents=True, exist_ok=True)
    return {
        "main": {"mode": f"{ArticatMode.test}"},
        "gcp": {"project": "test-gcp-project"},
        "fs": {
            "tmp_prefix": test_fs_prefix.joinpath("tmp"),
            "dev_prefix": test_fs_prefix.joinpath("dev"),
            "prod_prefix": test_fs_prefix.joinpath("prod"),
        },
        "bq": {
            "prod_dataset": "BQ_TEST_PROD_DATASET",
            "dev_dataset": "BQ_TEST_DEV_DATASET",
        },
    }


ArticatConfig.register_config(config_paths=(), config_dict=get_test_config())
