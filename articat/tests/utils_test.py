import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from dulwich import porcelain
from pytest import fixture

from articat.artifact import Artifact
from articat.fs_artifact import FSArtifact
from articat.tests.utils import TestFSArtifact
from articat.utils import utils
from articat.utils.typing import PathType
from articat.utils.utils import download_artifact, dummy_unsafe_cache, get_repo_and_hash


def get_source_path_that_looks_like_path_from_catalog() -> Path:
    p = Path(
        tempfile.mkdtemp(dir=TestFSArtifact.config.fs_prod_prefix, suffix="__ID__")
    ).joinpath("__PART__")
    p.mkdir(parents=True)
    return p


def test_happy_path():
    src = get_source_path_that_looks_like_path_from_catalog()
    src.joinpath("out-1.txt").write_text("sth-1")
    src.joinpath("out-2.txt").write_text("sth-2")
    a = TestFSArtifact(id="sth", files_pattern=f"{src}/*")
    dst = Path(tempfile.mktemp())
    r = download_artifact(a, dst)
    assert dst.joinpath("out-1.txt").read_text() == "sth-1"
    assert dst.joinpath("out-2.txt").read_text() == "sth-2"
    assert r == f"{dst}/*"


def test_download__empty():
    src = get_source_path_that_looks_like_path_from_catalog()
    a = TestFSArtifact(id="sth", files_pattern=f"{src}/**")
    dst = Path(tempfile.mktemp())
    with pytest.raises(ValueError, match="Nothing to copy in"):
        download_artifact(a, dst)


def test_download__weird_valid_pattern():
    src = get_source_path_that_looks_like_path_from_catalog()
    src.joinpath("part-1").write_text("sth-2")
    a = TestFSArtifact(id="sth", files_pattern=f"{src}/**part-1")
    dst = Path(tempfile.mktemp())
    r = download_artifact(a, dst)
    assert dst.joinpath("part-1").read_text() == "sth-2"
    assert r == f"{dst}/**part-1"


def test_download__weird_invalid_pattern():
    src = get_source_path_that_looks_like_path_from_catalog()
    src.joinpath("part-1").write_text("sth-2")
    a = TestFSArtifact(id="sth", files_pattern=f"{src}/*/*part-1")
    dst = Path(tempfile.mktemp())
    with pytest.raises(ValueError, match="Nothing to copy"):
        download_artifact(a, dst)


def test_download__single_file():
    src = get_source_path_that_looks_like_path_from_catalog()
    src.joinpath("out-1.txt").write_text("sth-1")
    src.joinpath("out-2.txt").write_text("sth-2")
    a = TestFSArtifact(id="sth", files_pattern=f"{src}/out-1.txt")
    dst = Path(tempfile.mktemp())
    r = download_artifact(a, dst)
    assert dst.joinpath("out-1.txt").read_text() == "sth-1"
    assert not dst.joinpath("out-2.txt").exists()
    assert r == f"{dst}/out-1.txt"


def test_download__embedded_dir():
    src = get_source_path_that_looks_like_path_from_catalog()
    src.joinpath("out-1.txt").write_text("sth-1")
    src.joinpath("out-2.txt").write_text("sth-2")
    src.joinpath("emb_dir").mkdir()
    src.joinpath("emb_dir").joinpath("out-3.txt").write_text("sth-3")
    a = TestFSArtifact(id="sth", files_pattern=f"{src}/**")
    dst = Path(tempfile.mktemp())
    r = download_artifact(a, dst.as_posix())
    assert dst.joinpath("out-1.txt").read_text() == "sth-1"
    assert dst.joinpath("out-2.txt").read_text() == "sth-2"
    assert dst.joinpath("emb_dir").joinpath("out-3.txt").read_text() == "sth-3"
    assert r == f"{dst}/**"


def test_download__existing_local_dir():
    src = get_source_path_that_looks_like_path_from_catalog()
    a = TestFSArtifact(id="sth", files_pattern=f"{src}/**")
    dst = Path(tempfile.mkdtemp())
    with pytest.raises(ValueError, match="Local directory already exists"):
        download_artifact(a, dst)


@fixture(scope="session")
def caching_artifact() -> FSArtifact:
    src = get_source_path_that_looks_like_path_from_catalog()
    src.joinpath("out-1.txt").write_text("sth-1")
    src.joinpath("out-2.txt").write_text("sth-2")
    return TestFSArtifact(
        id="sth",
        partition=datetime.utcnow(),
        created=datetime.utcnow(),
        files_pattern=f"{src}/out-1.txt",
    )


def test_cache__happy_path(caching_artifact: FSArtifact) -> None:
    cache_dir = Path(tempfile.mkdtemp())
    b = dummy_unsafe_cache(caching_artifact, cache_dir)
    assert b.endswith("out-1.txt")
    assert caching_artifact.files_pattern is not None
    assert caching_artifact.files_pattern not in b
    assert cache_dir.as_posix() in b
    c = dummy_unsafe_cache(caching_artifact, cache_dir)
    assert b == c


def test_cache__env_var(monkeypatch: MonkeyPatch, caching_artifact: FSArtifact) -> None:
    cache_dir = tempfile.mkdtemp()
    diff_cache_dir = tempfile.mkdtemp()
    monkeypatch.setenv("ARTICAT_CACHE_DIR", cache_dir)
    b = dummy_unsafe_cache(caching_artifact, diff_cache_dir)
    assert diff_cache_dir in b
    assert cache_dir not in b
    c = dummy_unsafe_cache(caching_artifact)
    assert b != c
    assert diff_cache_dir not in c
    assert cache_dir in c


def test_cache__diff_artifact_id(caching_artifact: FSArtifact) -> None:
    cache_dir = Path(tempfile.mkdtemp())
    b = dummy_unsafe_cache(caching_artifact, cache_dir)
    diff_art = caching_artifact.copy(update={"id": "sth_diff"})
    c = dummy_unsafe_cache(diff_art, cache_dir)
    assert cache_dir.as_posix() in b
    assert cache_dir.as_posix() in c
    assert b != c


def test_cache__diff_artifact_partition_version_created(
    monkeypatch: MonkeyPatch, caching_artifact: FSArtifact
) -> None:
    monkeypatch.setattr(Artifact, "_partition_str_format", "%Y%m%dT%H%M%S%f")
    cache_dir = Path(tempfile.mkdtemp())
    b = dummy_unsafe_cache(caching_artifact, cache_dir)
    diff_partition = caching_artifact.copy(update={"partition": datetime.utcnow()})
    diff_version = caching_artifact.copy(update={"version": "0.1.3"})
    diff_created = caching_artifact.copy(update={"created": datetime.utcnow()})
    c = dummy_unsafe_cache(diff_partition, cache_dir)
    d = dummy_unsafe_cache(diff_version, cache_dir)
    e = dummy_unsafe_cache(diff_created, cache_dir)

    assert all(cache_dir.as_posix() in x for x in [b, c, d, e])
    assert b != c != d != e


def test_cache__download_failure_scenario(
    caching_artifact: FSArtifact, monkeypatch: MonkeyPatch
) -> None:
    cache_dir = tempfile.mkdtemp()

    def raising_download(artifact: FSArtifact, local_dir: PathType) -> str:
        # we download everything
        str = download_artifact(artifact, local_dir)
        # and then simulate an error
        assert Path(str).exists()
        # we make sure at this stage cache dir is not empty
        assert cache_dir in str
        assert len(list(Path(cache_dir).glob("*"))) > 0
        raise ValueError("blah")

    monkeypatch.setattr(utils, "download_artifact", raising_download)
    with pytest.raises(ValueError, match="Cache download failed, make sure"):
        dummy_unsafe_cache(caching_artifact, cache_dir)

    assert len(list(Path(cache_dir).glob("*"))) == 0


def test_git__get_repo_curr_repo():
    repo_url, hash = get_repo_and_hash()
    assert "related-sciences/articat" in repo_url
    assert len(hash) > 0


def test_git__dirty_tree():
    repo_dir = tempfile.mkdtemp()
    r = porcelain.init(repo_dir)
    porcelain.commit(r, message="empty_commit")

    _, hash = get_repo_and_hash(repo_dir)
    assert "DIRTY" not in hash

    some_file = Path(repo_dir).joinpath("some_file")
    some_file.touch()
    _, hash = get_repo_and_hash(repo_dir)
    assert hash.endswith("DIRTY")
    some_file.unlink()
    _, hash = get_repo_and_hash(repo_dir)
    assert "DIRTY" not in hash

    some_file.touch()
    added, _ = porcelain.add(r, some_file)
    assert len(added) > 0
    _, hash = get_repo_and_hash(repo_dir)
    assert hash.endswith("DIRTY")
    porcelain.commit(r, message="commit some file")
    _, hash = get_repo_and_hash(repo_dir)
    assert "DIRTY" not in hash
