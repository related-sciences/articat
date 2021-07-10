from datetime import date
from pathlib import Path

from pytest import fixture

from articat.fs_artifact import FSArtifact
from articat.utils.notebook_utils import papermill_notebook

FIXTURE_DIR = Path(__file__).resolve().parent.joinpath("test_files")


@fixture
def dummy_notebook(tmpdir: str):
    return FIXTURE_DIR.joinpath("dummy_notebook.ipynb")


def test_papermill_notebook_default_params(caplog, dummy_notebook: Path):
    out = papermill_notebook(dummy_notebook)
    assert out.notebook_src == dummy_notebook
    assert out.notebook_exe.stat().st_size > 0
    assert out.notebook_html.stat().st_size > 0
    assert "foo artifact partition: None" in caplog.text


def test_papermill_notebook_custom_params(caplog, dummy_notebook: Path):
    out = papermill_notebook(
        dummy_notebook,
        params={
            "a": 3,
            "foo": FSArtifact.partitioned("foo", partition=date(1643, 1, 4), dev=True),
        },
    )
    assert out.notebook_src == dummy_notebook
    assert out.notebook_exe.stat().st_size > 0
    assert out.notebook_html.stat().st_size > 0
    assert "foo artifact partition: 1643-01-04" in caplog.text
