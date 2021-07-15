import re
import tempfile
from pathlib import Path

import fsspec
import pytest

from articat.artifact import ID
from articat.fs_artifact import FSArtifact
from articat.tests.utils import TestCatalog, TestFSArtifact


def test_fs_stage(uid: ID, tmpdir: str) -> None:
    tmpdir_path = Path(tmpdir).joinpath("foo")
    tmpdir_path.write_text("foo")

    with TestFSArtifact.versioned(uid, version="foo") as a:
        a.stage(tmpdir_path)
    assert Path(a.joinpath("foo")).read_text() == "foo"


def test_fs_artifact_join_paths_works(uid: ID) -> None:
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        assert a._file_prefix is not None
        p = a.joinpath("data.dat")
        with fsspec.open(p, "w") as f:
            f.write("ala ma kota")
        a.files_pattern = None
        assert p == f"{a.staging_file_prefix}/data.dat"
    assert a._file_prefix is None
    p = a.joinpath("data.dat")
    assert p == f"{a.main_dir}/data.dat"
    assert Path(p).read_text() == "ala ma kota"


def test_catalog_main_path_is_valid(uid: ID) -> None:
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.files_pattern = None
    a = TestCatalog.latest_partition(uid, model=TestFSArtifact)
    assert "**" not in a.main_dir
    assert a.files_pattern
    assert a.files_pattern.endswith("/**")
    assert a.files_pattern.startswith(a.main_dir)
    assert a.main_dir == a.files_pattern.replace("/**", "")


def test_catalog_main_path_is_valid_in_dev_mode(uid: ID) -> None:
    TestFSArtifact.write_dummy_versioned(f"_dev_{uid}", "0.1.0")
    a = TestCatalog.latest_partition(f"_dev_{uid}", model=TestFSArtifact)
    assert "**" not in a.main_dir
    assert a.files_pattern
    assert a.files_pattern.endswith("/output.txt")
    assert a.files_pattern.startswith(a.main_dir)
    assert a.main_dir == a.files_pattern.replace("/output.txt", "")


def test_catalog_no_files_pattern_includes_everything(uid: ID) -> None:
    with TestFSArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.files_pattern = None
    output = TestCatalog.get(uid, version="0.1.0", model=FSArtifact).files_pattern
    assert output is not None
    assert output.endswith("/**")


def test_fail_on__missing_output(uid: ID) -> None:
    with pytest.raises(ValueError, match="There are no files in the output"):
        with TestFSArtifact.partitioned(uid) as art:
            out_dir = Path(tempfile.mkdtemp())
            art.files_pattern = f"{out_dir.as_posix()}/*"


def test_publish_file_works_for_local() -> None:
    fs = fsspec.filesystem("file")
    prefix = f"{tempfile.mkdtemp()}"
    fs.touch(f"{prefix}/sth.txt")
    dst_prefix = f"{tempfile.mkdtemp()}"
    TestFSArtifact._publish_file(fs, f"{prefix}/sth.txt", prefix, dst_prefix)
    assert Path(f"{dst_prefix}/sth.txt").exists()


def test_fs_scheme_regex() -> None:
    assert (
        re.sub(TestFSArtifact._fs_scheme_regex, "", "gs://test-sth/foobar")
        == "test-sth/foobar"
    )
    assert (
        re.sub(TestFSArtifact._fs_scheme_regex, "", "gs://test-sth/foobar://fds[.*]")
        == "test-sth/foobar://fds[.*]"
    )
