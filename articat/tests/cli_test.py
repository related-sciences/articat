from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Iterator

from _pytest.capture import CaptureFixture
from _pytest.monkeypatch import MonkeyPatch

from articat.artifact import Artifact, Metadata
from articat.catalog import Catalog
from articat.cli import CLI
from articat.fs_artifact import FSArtifact
from articat.tests.utils import TestFSArtifact


def get_dummy_artifact() -> Artifact:
    dt = datetime.combine(date(1642, 12, 25), time.min)
    return FSArtifact(
        id="sth",
        partition=dt,
        created=dt,
        version="0.1.0",
        metadata=Metadata(),
        files_pattern="/tmp/bah",
    )


def get_lookup(a: Artifact):
    def my_lookup(**_: Any) -> Iterator[FSArtifact]:
        yield a

    return my_lookup


def test_happy_path(monkeypatch: MonkeyPatch, capsys: CaptureFixture) -> None:
    monkeypatch.setattr(Catalog, "lookup", get_lookup(get_dummy_artifact()))
    CLI.lookup()
    out, _ = capsys.readouterr()
    expected = '"files_pattern": "/tmp/bah"'
    assert expected in out
    assert "metadata" not in out
    CLI.lookup(no_trunc=True)
    out, _ = capsys.readouterr()
    assert "metadata" in out


def test_open_artifact_id(monkeypatch: MonkeyPatch, caplog: CaptureFixture) -> None:
    a = get_dummy_artifact()

    def dummy_open_browser():
        print(f"Would open browser for {a.spec()}")

    object.__setattr__(a, "open_browser", dummy_open_browser)
    monkeypatch.setattr(Catalog, "lookup", get_lookup(a))
    CLI.open(id="sth")
    assert f"Opening artifact: {a.spec()}" in caplog.text


def test_local_dump(tmpdir: str) -> None:
    tmpdir_path = Path(tmpdir).joinpath("foo")
    tmpdir_path.write_text("txt")
    desc = "description"
    a = CLI.dump(
        id="test",
        local_path=tmpdir_path.as_posix(),
        version="0.1.0",
        description=desc,
        dev=True,
        _test_artifact=TestFSArtifact.partitioned(id="test", dev=True),
    )
    assert a.metadata.description == desc
    assert Path(a.joinpath("foo")).read_text() == "txt"
