import inspect
import logging
import os
import re
import shutil
import tempfile
import time
from hashlib import md5
from pathlib import Path
from typing import Optional, Tuple, Type

import fsspec
import pandas as pd
from dulwich import porcelain
from dulwich.repo import Repo
from fsspec import AbstractFileSystem

from articat.artifact import ID
from articat.fs_artifact import FSArtifact
from articat.pandas_utils import stringify_lists
from articat.path_utils import fsspec_copyfile, get_root_path
from articat.py_utils import lazy
from articat.typing import PathType

logger = logging.getLogger(__name__)


def get_repo_and_hash(
    repo_path: Optional[PathType] = None, remote_location: str = "origin"
) -> Tuple[str, str]:
    """
    Returns git remote repository URL and HEAD hash. If the tree at
    HEAD is dirty, the hash will include "-DIRTY" suffix.
    """
    repo = Repo(repo_path or get_root_path())

    t0 = time.time()
    # if the repository contains a lot of dirty directories this call may take a long time
    sts = porcelain.status(repo)
    status_call_time = time.time() - t0
    if status_call_time > 5:
        logger.warning(
            f"Git status call took {status_call_time} seconds, something might be "
            "wrong with your git repo, like large number of untracked directories, "
            "see https://github.com/dulwich/dulwich/issues/835"
        )
    staged_clean = all(len(sts.staged[c]) == 0 for c in ["add", "delete", "modify"])
    unstaged_clean = len(sts.unstaged) == 0
    untracked_clean = len(sts.untracked) == 0
    tree_dirty = not all((staged_clean, unstaged_clean, untracked_clean))

    if tree_dirty:
        head_hash = f"{repo.head().decode('UTF-8')}-DIRTY"
    else:
        head_hash = repo.head().decode("UTF-8")

    remote_url = porcelain.get_remote_repo(repo, remote_location=remote_location)[1]
    return remote_url, head_hash


def get_relative_call_site(frames_back: int) -> Optional[Tuple[str, int]]:
    """
    Returns relative path (to the repo root) of the call site file.

    :param frames_back: number of frames to look back in the call stack for the
                        call site of interest. For example 1 is the call site of
                        the get_relative_call_site itself, 2 is the call site of
                        the function that called get_relative_call_site and so on.
    """
    caller_frame = inspect.stack()[frames_back]
    caller_fname_path = Path(caller_frame.filename)
    caller_lineno = caller_frame.lineno
    root_path = get_root_path()
    if root_path not in caller_fname_path.parents:
        logger.warning(
            f"Was called from {caller_fname_path.absolute().as_posix()} that appears to live "
            "outside the repository, can't resolve the relative path"
        )
        return None
    else:
        return caller_fname_path.relative_to(root_path).as_posix(), caller_lineno


def download_artifact(artifact: FSArtifact, local_dir: PathType) -> str:
    """
    Downloads artifact outputs to local directory.
    """
    if isinstance(local_dir, str):
        local_dir = Path(local_dir)
    if local_dir.exists():
        raise ValueError(
            "Local directory already exists, please point at a non existing directory"
        )
    local_dir.mkdir(parents=True)
    protocol = artifact._get_protocol(artifact.files_pattern)
    src_fs: AbstractFileSystem = fsspec.filesystem(protocol)
    assert artifact.files_pattern is not None
    assert artifact.main_dir is not None
    prefix = artifact.main_dir
    # Note: glob results don't have fs scheme
    prefix_no_scheme = re.sub(FSArtifact._fs_scheme_regex, "", prefix)
    to_copy = src_fs.glob(artifact.files_pattern)
    if len(to_copy) == 0:
        raise ValueError(f"Nothing to copy in `{artifact.files_pattern}`")
    for f in to_copy:
        assert (
            prefix_no_scheme in f
        ), f"prefix {prefix_no_scheme}, should be present in the file path {f}"
        dst = f.replace(prefix_no_scheme, local_dir.as_posix())
        if src_fs.isdir(f):
            Path(dst).mkdir()
        else:
            src_fs.download(f, dst)
    suffix = artifact.files_pattern.replace(prefix, "")
    return f"{local_dir.as_posix()}{suffix}"


def dummy_unsafe_cache(
    artifact: FSArtifact, cache_dir: Optional[PathType] = None
) -> str:
    """
    This is an unsafe implementation of local FS caching for FSArtifacts.

    Context:
    We don't want to build caching into jia, since it's a hard problem to
    solve properly. It as a helper method, and this should be used at your
    own risk (and definitely not in the production environment).

    Example:

    ```
    a = Catalog.latest_partition("id")
    df = spark.read.text(dummy_unsafe_cache(a))
    ```

    :param artifact: FSArtifact to return data for
    :param cache_dir: cache dir location, can be provided either by this argument
        or environment variable RS_CACHE_DIR. Argument takes precedence over
        environment variable.
    :return: string representing the local version of `artifact.files_pattern`
    """
    if cache_dir is None:
        cache_dir = os.environ.get("RS_CACHE_DIR")
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
        else:
            raise ValueError(
                "No cache_dir set and no RS_CACHE_DIR env variable, don't know where to store cache"
            )
    assert cache_dir is not None
    cache_dir_path: Path = Path(cache_dir) if isinstance(cache_dir, str) else cache_dir
    if not cache_dir_path.exists():
        logger.info(f"Creating cache dir: {cache_dir_path.as_posix()}")
        cache_dir_path.mkdir(parents=True)
    m = md5()
    assert artifact.id is not None
    m.update(artifact.id.encode("UTF-8"))
    assert artifact.partition is not None
    m.update(
        artifact.partition.strftime(FSArtifact._partition_str_format).encode("UTF-8")
    )
    assert artifact.created is not None
    m.update(
        artifact.created.strftime(FSArtifact._partition_str_format).encode("UTF-8")
    )
    m.update((artifact.version and artifact.version.encode("UTF-8")) or b"")
    hex = m.hexdigest()

    cache_entry = cache_dir_path.joinpath(hex)
    if cache_entry.exists():
        logger.info(f"Cache hit for artifact {artifact.id}, returning local data ...")
        # TODO (rav): use main_dir when it's available
        assert artifact.files_pattern is not None
        assert artifact.main_dir is not None
        suffix = artifact.files_pattern.replace(artifact.main_dir, "")
        return f"{cache_entry.as_posix()}{suffix}"
    else:
        logger.info(f"Cache miss for artifact {artifact.id}, downloading data ...")
        try:
            return download_artifact(artifact, cache_entry)
        except BaseException as e:
            logger.exception(
                f"Cache download failed, will do my best to invalidate cache entry {cache_entry.as_posix()}"
            )
            try:
                shutil.rmtree(cache_entry)
            finally:
                raise ValueError(
                    f"Cache download failed, make sure {cache_entry.as_posix()} does not exits!"
                ) from e


def stage_dev_data(
    aid: ID,
    desc: str,
    excel_dump_pattern: Optional[str] = None,
    artifact_cls: Type[FSArtifact] = FSArtifact,
    **pd_datasets: pd.DataFrame,
) -> FSArtifact:
    """
    Dumps development data, to ease data sharing etc. Dumps arbitrary number of Pandas Dataframes
    to the dev Catalog. You can also choose to allow certain Dataframes to be written to Excel
    file. Keep in mind that development data is deleted after 30 days.
    :param aid: artifact id, must start with _dev
    :param desc: artifact description
    :param excel_dump_pattern: "allow pattern" to select dataframes for Excel dump, use ".*" to select all.
    :param pd_datasets: kwargs of the Dataframes, keys will be the file names (or sheet names in Excel).
    :param artifact_cls: artifact class, right now used for tests.
    :return: artifact

    Example:

    ```
    stage_dev_data("_dev_abbvie_progressions",
                   "Dump progressions",
                   excel_dump_allow_re="progress_stats",
                   progress_stats=risk_ratio_df,
                   pharma_genetics=combined_df)
    ```

    Will dump two Pandas Dataframes, and one of them in Excel.
    """
    if not aid.startswith("_dev"):
        raise ValueError("Dev artifact id must start with `_dev`")
    if len(pd_datasets) == 0:
        raise ValueError("Must provide at least one pandas dataframe to stage")
    if not desc:
        raise ValueError("You must provide description")
    with artifact_cls.partitioned(aid) as a:
        # excel writer fails if there is no data written, so we wrap it in a lazy
        # thus only create it when it's needed
        # the type ignore is needed due to mypy bug: https://github.com/python/mypy/issues/9590

        def create_writer() -> Tuple[pd.ExcelWriter, PathType]:
            path = tempfile.mktemp(suffix=".xlsx")
            return (
                pd.ExcelWriter(path=path, writer="openpyxl"),
                path,
            )

        lz_xlsx_writer = lazy(create_writer)
        for n, df in pd_datasets.items():
            df.to_parquet(
                f"{a.staging_file_prefix}/{n}.snappy.parquet",
                compression="snappy",
                index=False,
            )
            if excel_dump_pattern and re.compile(excel_dump_pattern).match(n):
                df = df.applymap(stringify_lists)
                df.to_excel(
                    lz_xlsx_writer()[0], freeze_panes=(1, 0), index=False, sheet_name=n
                )
        if excel_dump_pattern and any(
            re.compile(excel_dump_pattern).match(n) for n in pd_datasets.keys()
        ):
            writer, path = lz_xlsx_writer()
            writer.close()
            # py3.9 has removeprefix, we should use that when we can
            xlsx_filename = aid[6:] if aid.startswith("_dev_") else aid[5:]
            fsspec_copyfile(str(path), f"{a.staging_file_prefix}/{xlsx_filename}.xlsx")
        a.metadata.description = desc
    files = "\n".join(fsspec.open(a.main_dir).fs.glob(a.files_pattern))
    logger.warning(f"Data dumped:\n{files}")
    return a
