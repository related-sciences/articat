from pathlib import Path
from typing import ClassVar

import pytest
from pytest import fixture

from articat.artifact import ID
from articat.notebook_artifact import NotebookArtifact
from articat.tests.utils import TestCatalog, TestFSArtifactMixin

pytestmark = pytest.mark.datastore_emulated

FIXTURE_DIR = Path(__file__).resolve().parent.joinpath("test_files")


@fixture
def dummy_notebook(tmpdir: str):
    return FIXTURE_DIR.joinpath("dummy_notebook.ipynb")


class TestNotebookArtifact(TestFSArtifactMixin, NotebookArtifact):
    """NotebookArtifact class for testing purposes"""

    __test__: ClassVar[bool] = False


def test_notebook_artifact(uid: ID, dummy_notebook: Path) -> None:
    with TestNotebookArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.stage_notebook(dummy_notebook, params={"a": 3})
        a.files_pattern = None

    out = a.fetch(TestCatalog)
    assert Path(out.joinpath("output.html")).stat().st_size > 0


def test_notebook_artifact_custom(uid: ID, tmpdir) -> None:
    custom_notebook_html = Path(tmpdir).joinpath("report_foo.html")
    html = "<html>FOO</html>"
    custom_notebook_html.write_text(html)
    with TestNotebookArtifact.dummy_versioned_ctx(uid, "0.1.0") as a:
        a.stage_rendered_notebook(custom_notebook_html.as_posix(), filename=None)
        a.files_pattern = None

    out = a.fetch(TestCatalog)
    assert Path(out.joinpath("report_foo.html")).read_text() == html
