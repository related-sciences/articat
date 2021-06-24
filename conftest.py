import uuid
from pathlib import Path

import pytest
from _pytest.config import Config
from _pytest.config.argparsing import Parser


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
