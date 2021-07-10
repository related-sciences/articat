import inspect
import logging
import os
import re
import shutil
import time
from hashlib import md5
from pathlib import Path
from typing import Optional, Tuple

import fsspec
from dulwich import porcelain
from dulwich.repo import Repo
from fsspec import AbstractFileSystem

from articat.artifact import Artifact
from articat.fs_artifact import FSArtifact
from articat.utils.path_utils import get_root_path
from articat.utils.typing import PathType

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
