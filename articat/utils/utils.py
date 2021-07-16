import inspect
import logging
import os
import re
import shutil
import subprocess
from hashlib import md5
from pathlib import Path
from typing import Optional, Tuple

import fsspec
from fsspec import AbstractFileSystem

from articat.artifact import Artifact
from articat.fs_artifact import FSArtifact
from articat.utils.typing import PathType

logger = logging.getLogger(__name__)


def _git_tree_is_dirty() -> bool:
    status_out = subprocess.check_output(
        ["git", "status", "--short", "--untracked-files=all", "--ignored=no"]
    )
    # NOTE: if status out is empty, tree is clean
    return len(status_out) > 0


def _git_get_head_hash() -> str:
    return (
        subprocess.check_output(["git", "rev-parse", "--verify", "HEAD"])
        .decode()
        .strip()
    )


def _git_get_remote_url(remote: str) -> str:
    return (
        subprocess.check_output(["git", "remote", "get-url", remote]).decode().strip()
    )


def get_repo_and_hash(remote_location: Optional[str] = None) -> Tuple[str, str]:
    """
    Returns git remote repository URL and HEAD hash. If the tree at
    HEAD is dirty, the hash will include "-DIRTY" suffix. Expect the CWD
    to be within the git repository.
    """
    if _git_tree_is_dirty():
        head_hash = f"{_git_get_head_hash()}-DIRTY"
    else:
        head_hash = _git_get_head_hash()

    remote_location = (
        remote_location or os.environ.get("ARTICAT_GIT_REMOTE") or "origin"
    )
    return _git_get_remote_url(remote_location), head_hash


def get_call_site(frames_back: int) -> Optional[Tuple[str, int]]:
    """
    Returns path of the call site file, and line number

    :param frames_back: number of frames to look back in the call stack for the
                        call site of interest. For example 1 is the call site of
                        the get_relative_call_site itself, 2 is the call site of
                        the function that called get_relative_call_site and so on.
    """
    caller_frame = inspect.stack()[frames_back]
    caller_fname_path = Path(caller_frame.filename)
    caller_lineno = caller_frame.lineno
    return caller_fname_path.as_posix(), caller_lineno


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
    We don't want to build caching into articat, since it's a hard problem to
    solve properly. This is a helper method, and should be used at your
    own risk (and definitely not in the production environment).

    Example:

    ```
    a = Catalog.latest_partition("id")
    df = spark.read.text(dummy_unsafe_cache(a))
    ```

    :param artifact: FSArtifact to return data for
    :param cache_dir: cache dir location, can be provided either by this argument
        or environment variable ARTICAT_CACHE_DIR. Argument takes precedence over
        environment variable.
    :return: string representing the local version of `artifact.files_pattern`
    """
    if cache_dir is None:
        cache_dir = os.environ.get("ARTICAT_CACHE_DIR")
        if cache_dir is not None:
            cache_dir = Path(cache_dir)
        else:
            raise ValueError(
                "No cache_dir set and no ARTICAT_CACHE_DIR env variable, don't know where to store cache"
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
        artifact.partition.strftime(Artifact._partition_str_format).encode("UTF-8")
    )
    assert artifact.created is not None
    m.update(artifact.created.strftime(Artifact._partition_str_format).encode("UTF-8"))
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
