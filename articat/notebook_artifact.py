from typing import Any, Dict, Optional

import fsspec
from gcsfs import GCSFileSystem

from articat.fs_artifact import FSArtifact
from articat.utils.notebook_utils import papermill_notebook
from articat.utils.path_utils import fsspec_copyfile, to_pathlib
from articat.utils.typing import PathType


class NotebookArtifact(FSArtifact):
    """
    A simple Notebook flavour of the FSArtifact. Each notebook artifact,
    should hold a **single** notebook output, and the notebook should be
    staged via the `stage_notebook` method.
    """

    def browser_url(self, filename: str = "output.html") -> str:
        """
        Returns the URL of the HTML output of the artifact. If you have
        a custom HTML report specify `filename`.
        """
        if self.main_dir.startswith("gs://"):
            auth_url_root = "https://storage.cloud.google.com"
            path = self.main_dir.lstrip("gs://")
            return f"{auth_url_root}/{path}/{filename}"
        else:
            return super().browser_url()

    def stage_rendered_notebook(
        self, report_path: PathType, filename: Optional[str] = "output.html"
    ) -> "NotebookArtifact":
        """
        Stages pre-rendered notebook. Default filename is `output.html`, if you want to
        keep original filename, set `filename` to None.
        """
        report_path_real = to_pathlib(report_path)
        html_output = self.joinpath(filename or report_path_real.name)
        fsspec_copyfile(report_path_real.as_posix(), html_output)

        fs = fsspec.open(html_output).fs
        if isinstance(fs, GCSFileSystem):
            # NOTE: we want to be able to open the HTML output in the browser
            #       from GCS, and thus need to adjust the content-type
            fs.setxattrs(path=html_output, content_type="text/html")
        return self

    def stage_notebook(
        self, notebook_path: PathType, params: Dict[str, Any] = {}
    ) -> "NotebookArtifact":
        """
        Allows to easily stage a notebook within this artifact. It saves the original
        notebook (with original name), the executed notebook with _executed suffix, and
        the HTML output in `output.html`.
        """
        nb_out = papermill_notebook(to_pathlib(notebook_path), params)
        fsspec_copyfile(
            nb_out.notebook_src.as_posix(), self.joinpath(nb_out.notebook_src.name)
        )
        fsspec_copyfile(
            nb_out.notebook_exe.as_posix(), self.joinpath(nb_out.notebook_exe.name)
        )
        return self.stage_rendered_notebook(nb_out.notebook_html)
