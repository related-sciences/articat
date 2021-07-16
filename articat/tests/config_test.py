import tempfile
from datetime import date
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from articat.config import ArticatConfig, ArticatMode
from articat.tests.utils import TestCatalog, TestFSArtifact

pytestmark = pytest.mark.datastore_emulated


def test_config_overwrite(uid: str):
    # Default Artifacts use default config
    assert ArticatConfig.fs_prod_prefix() in TestFSArtifact.config().fs_prod_prefix()

    custom_text = "SUPER_CUSTOM"

    # Get new FS paths:
    test_fs_prefix = Path(tempfile.mkdtemp(suffix=custom_text))
    for d in ("tmp", "dev", "prod"):
        test_fs_prefix.joinpath(d).mkdir(parents=True, exist_ok=True)

    custom_config = ArticatConfig(
        config_paths=(),
        config_dict=dict(
            ArticatConfig._config,
            fs={
                "tmp_prefix": test_fs_prefix.joinpath("tmp"),
                "dev_prefix": test_fs_prefix.joinpath("dev"),
                "prod_prefix": test_fs_prefix.joinpath("prod"),
            },
        ),
    )
    today = date.today()
    a1 = TestFSArtifact.partitioned(uid, today)
    a2 = TestFSArtifact.partitioned(f"{uid}_2", today, config=custom_config)

    assert custom_text not in a1.config().fs_prod_prefix()
    assert custom_text in a2.config().fs_prod_prefix()

    dummy_path = Path(tempfile.mktemp())
    dummy_path.touch()
    for i in (a1, a2):
        with i as a:
            a.stage(dummy_path)

    a1_dir = TestCatalog.get(a1.id, partition=today, model=TestFSArtifact).files_dir
    a2_dir = TestCatalog.get(a2.id, partition=today, model=TestFSArtifact).files_dir

    assert a1_dir and custom_text not in a1_dir
    assert a2_dir and custom_text in a2_dir


def test_config_env_variable(monkeypatch: MonkeyPatch):
    test_config = ArticatConfig._config
    test_config_mode = ArticatConfig.mode()
    assert test_config_mode == ArticatMode.gcp_datastore

    custom_config_path = Path(tempfile.mktemp())
    custom_config_path.write_text("[main]\n" "mode = local\n")

    try:
        with monkeypatch.context() as m:
            m.setenv("ARTICAT_CONFIG", custom_config_path.as_posix())
            ArticatConfig.register_config()
            assert ArticatConfig.mode() == ArticatMode.local
            assert ArticatConfig.mode() != test_config_mode
    finally:
        ArticatConfig._config = test_config
