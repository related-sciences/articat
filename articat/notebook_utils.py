import logging
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, cast

from google.cloud.bigquery._helpers import _datetime_to_json
from nbconvert import HTMLExporter
from nbformat.v4 import nbjson
from papermill import execute_notebook
from papermill.translators import PythonTranslator, papermill_translators

from articat.artifact import Artifact

logger = logging.getLogger(__name__)


@dataclass
class PapermillOutput:
    """Value class to hold output of the papermill_notebook function"""

    notebook_src: Path
    """Holds the path to the source notebook"""
    notebook_exe: Path
    """Holds the path to the papermill executed notebook"""
    notebook_html: Path
    """Holds the path to the HTML export of the executed notebook"""


def papermill_notebook(
    notebook_path: Path, params: Dict[str, Any] = {}
) -> PapermillOutput:
    """
    Uses papermill to execute notebook with potential parameters. Read the papermill documentation
    for a tutorial on the parameter handling. It saves executed notebook and an HTML report in a
    temporary directory. The original notebook is untouched.
    """
    tmp_dir = Path(tempfile.mkdtemp())
    logger.debug(
        f"Working/temp directory for papermill on {notebook_path} with {params} will be {tmp_dir}"
    )

    assert notebook_path.name.endswith(
        ".ipynb"
    ), f"Invalid notebook file suffix in {notebook_path.name}"
    notebook_exe_path = tmp_dir.joinpath(
        notebook_path.name.replace(".ipynb", "_executed.ipynb")
    )

    logger.debug(f"Temp executed notebook will be saved in {notebook_exe_path}")

    nb_node = execute_notebook(
        input_path=notebook_path,
        output_path=notebook_exe_path,
        parameters=params,
        log_output=True,
    )

    html_exporter = HTMLExporter()
    html_body, resources = html_exporter.from_notebook_node(nb_node)

    notebook_html_path = tmp_dir.joinpath("output.html")
    notebook_html_path.write_text(html_body)

    out = PapermillOutput(
        notebook_src=notebook_path,
        notebook_exe=notebook_exe_path,
        notebook_html=notebook_html_path,
    )

    logger.info(
        f"Notebook {out.notebook_src} execution saved in {out.notebook_exe}, HTML export in {out.notebook_html}"
    )
    return out


class RSNBJsonEncoder(nbjson.BytesEncoder):  # type: ignore[misc]
    """
    RS flavour of the Notebook JsonEncoder.

    This is required for notebook API to be able to serialize
    Artifact objects, this is being called by papermill.
    """

    def default(self, obj: Any) -> str:
        if isinstance(obj, Artifact):
            # NOTE: we really only need the spec, it's assumed that
            #       the notebook will call fetch() on the artifacts
            #       to retrieve metadata at the runtime.
            return cast(str, super().encode(obj.spec()))
        elif isinstance(obj, datetime):
            return cast(str, _datetime_to_json(obj))
        elif isinstance(obj, Mapping):
            return cast(str, super().encode(dict(obj)))
        else:
            return cast(str, super().default(obj))


nbjson.BytesEncoder = RSNBJsonEncoder
# NOTE: here we overwrite the default BytesEncoder in the notebook internals,
#       afaiu there's no mechanism to do it gracefully, thus we need this
#       uncivilized, brute method above


class RSPythonTranslator(PythonTranslator):  # type: ignore[misc]
    """
    RS flavour of the papermill translator.

    My current understanding of the Translator: it takes a python object,
    and should return a stringified code that would recreate it the
    notebook cell. We need a custom Translator to teach it how to recreate
    the Artifact object.
    """

    @classmethod
    def translate_artifact(cls, val: Artifact) -> str:
        # NOTE: here we need to translate an existing Artifact object into
        #       a line of code (str) that would recreate it in the notebook.
        #       It is assumed that the notebook should have all the necessary
        #       imports, otherwise it will fail at the runtime with undefined
        #       symbol. It's a fair assumption, given that the user should
        #       provide default Artifact spec in the parameter cell.
        spec = cls.translate(val.spec())
        return f"{val.__class__.__name__}(**{spec})"

    @classmethod
    def translate(cls, val: Any) -> str:
        if isinstance(val, Artifact):
            return cls.translate_artifact(val)
        else:
            return cast(str, super().translate(val))


papermill_translators.register("python", RSPythonTranslator)
# NOTE: here we overwrite the default python translator
