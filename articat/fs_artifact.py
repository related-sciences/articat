import logging
import os
import re
import time
import uuid
from functools import lru_cache
from multiprocessing.pool import ThreadPool
from pathlib import Path
from types import TracebackType
from typing import ClassVar, List, Optional, Type

import fsspec
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem

from articat.artifact import Artifact

logger = logging.getLogger(__name__)


class FSArtifact(Artifact):
    """
    File/object based artifact.

    Usage:
     * use to store file/object artifacts
     * use `staging_file_prefix` location to stage your data
     * you should use it in the context of with-statement
     * use dev argument to indicate dev mode

    Example:
        with FSArtifact.partitioned("id", date.today(), dev=True) as artifact:
            with fsspec.open(f"{artifact.staging_file_prefix}/out.txt", "w") as fd:
                fd.write("hello world")
    """

    # Regex of the files/objects produced by this Artifact, needs to be
    # set by the user
    files_pattern: Optional[str] = None
    # Represents the "root" of the artifacts file structure, populated
    # automatically
    files_dir: Optional[str] = None

    # PRIVATE fields:
    _file_prefix: Optional[str] = None
    _fs_scheme_regex: ClassVar[str] = r"^.*?://"

    @property
    def staging_file_prefix(self) -> str:
        """
        This is your staging location for outputs. When you are done make sure to
        update `files_pattern` with a pattern/glob that includes all the outputs,
        within the `staging_file_prefix`.
        """
        if not self._file_prefix:
            raise ValueError(
                "staging_file_prefix is unset, if you are creating a new "
                "artifact make sure to use it within with-statement "
                "context. Artifacts retrieved from Catalog do not "
                "have staging_file_prefix set, and you are probably looking "
                "for `files_pattern` field."
            )
        else:
            return self._file_prefix

    @property
    def main_dir(self) -> str:
        """Returns a "main path" to the artifact.

        The main path is where all the files are stored, user has
        and full freedom on the structure inside the main directory,
        and `files_pattern` is a pattern within the main path.
        """
        # to not worry about presence of scheme, we remove the prefix
        # and add it back
        if self.files_dir:
            return self.files_dir
        # TODO: remove this code path
        # This is the legacy handling of the main_dir, where we gather
        # main dir from the files_pattern
        assert self.files_pattern is not None
        if self.is_dev():
            no_prefix = self.files_pattern.replace(self.config.fs_dev_prefix, "")
            return self.config.fs_dev_prefix + "/".join(no_prefix.split("/")[:4])
        else:
            no_prefix = self.files_pattern.replace(self.config.fs_prod_prefix, "")
            return self.config.fs_prod_prefix + "/".join(no_prefix.split("/")[:3])

    def joinpath(self, *parts: str) -> str:
        """Helper to easily construct paths within the Artifact"""
        # NOTE: ATTOW, fsspec.open still requires str as path, and since
        #       fsspec is our de facto FS interface, here we still return
        #       string as path (instead of for example pathlib.Path)
        #       Related: https://github.com/intake/filesystem_spec/issues/114
        if self._file_prefix is not None:
            return os.path.join(self.staging_file_prefix, *parts)
        else:
            return os.path.join(self.main_dir, *parts)

    @staticmethod
    @lru_cache
    def _worker_pool() -> ThreadPool:
        # Note: this worker pool is used for file ops
        return ThreadPool()

    @staticmethod
    def _publish_file(
        fs: AbstractFileSystem, src: str, staging_prefix: str, dst_prefix: str
    ) -> str:
        if isinstance(fs, LocalFileSystem):
            # This is for tests etc
            if not fs.exists(dst_prefix):
                fs.mkdir(dst_prefix)
        else:
            staging_prefix = re.sub(FSArtifact._fs_scheme_regex, "", staging_prefix)
            dst_prefix = re.sub(FSArtifact._fs_scheme_regex, "", dst_prefix)
        if staging_prefix not in src:
            raise ValueError(
                f"Looks like staged file pattern is outside of the staging space: {staging_prefix} "
                f"you can only publish data from within staging prefix."
            )
        dst = src.replace(staging_prefix, dst_prefix)
        # We copy not move, to keep the staging intact
        logger.debug(f"Will publish `{src}` to `{dst}`")
        try:
            fs.cp(src, dst)
        except FileNotFoundError as e:
            if isinstance(fs, GCSFileSystem):
                if fs.isdir(src):
                    logger.debug(
                        f"Skipping file not found for a fake directory on GCS: {src}"
                    )
                    return dst
            raise e
        return dst

    def build(self) -> "FSArtifact":
        """
        Builds this artifact. In most cases you should use artifact within with-statement
        context which takes care of building and saving artifacts for you, and that's the
        recommended usage.
        """
        # This is mostly to test that metadata doesn't have any illegal types before
        # we copy any files etc
        self._test_artifact_serialization()
        if not self.files_pattern:
            std_output = f"{self.staging_file_prefix}/**"
            logger.warning(f"Will publish whole staging directory: {std_output}")
            self.files_pattern = std_output
        fs = fsspec.filesystem(self._get_protocol(self.files_pattern))

        # Note: this could potentially be problematic for very large number of
        # files, as in thousands of files, when that becomes and issue, we
        # can optimize it.
        start = time.time()
        output_files: List[str] = fs.glob(self.files_pattern)
        logger.debug(f"Temp output glob took: {time.time() - start} s")

        if len(output_files) == 0:
            raise ValueError(
                f"There are no files in the output `{self.files_pattern}`, aborting"
            )

        # Next:
        # * we copy files from staging area
        # * add a _SUCCESS marker file
        # * dump all the metadata about this artifact in a _MANIFEST.json file
        actual_prefix = self._get_file_prefix(tmp=False)
        start = time.time()
        self._worker_pool().map(
            lambda p: self._publish_file(
                fs, p, self.staging_file_prefix, actual_prefix
            ),
            output_files,
        )
        self.files_pattern = self.files_pattern.replace(
            self.staging_file_prefix, actual_prefix
        )
        self.files_dir = actual_prefix
        logger.debug(f"Output files published in: {time.time() - start}s")
        if fs.exists(f"{actual_prefix}/_MANIFEST.json"):
            raise ValueError(
                "Manifest file `_MANIFEST.json` already present in the staged files - this is not supported"
            )
        with fs.open(f"{actual_prefix}/_MANIFEST.json", "w") as m:
            m.write(self.json(exclude=self._exclude_private_fields()))
        try:
            # truncate false, means if the file already exists do not
            # truncate it, which is what we want. If the file
            # exists, `touch` on AbstractFS and GCSFS throws
            # NotImplementedError, in which case we just double check
            # if the SUCCESS marker exist, and raise if it not
            fs.touch(f"{actual_prefix}/_SUCCESS", truncate=False)
        except NotImplementedError:
            assert fs.exists(
                f"{actual_prefix}/_SUCCESS"
            ), "We could not create a SUCCESS marker and it does not exist"
        return self

    def _get_file_prefix(self, tmp: bool = True) -> str:
        if not self.created:
            raise ValueError("created must be set")
        if not self.id:
            raise ValueError("ID must be set")

        partition_path = self.created.strftime(Artifact._partition_str_format)
        if tmp:
            return (
                f"{self.config.fs_tmp_prefix}/{self.id}/{partition_path}/{uuid.uuid4()}"
            )
        elif self.is_dev():
            return (
                f"{self.config.fs_dev_prefix}/{self.id}/{partition_path}/{uuid.uuid4()}"
            )
        else:
            return f"{self.config.fs_prod_prefix}/{self.id}/{partition_path}/{uuid.uuid4()}"

    @staticmethod
    def _get_protocol(path: Optional[str]) -> str:
        if not path:
            raise ValueError("Path should not be None")
        else:
            return path.split(":")[0] if ":" in path else "file"

    def __enter__(self) -> "FSArtifact":
        r: FSArtifact = super().__enter__()

        if not r._file_prefix:
            # This kind of setter flavour is required by pydantic for private fields
            object.__setattr__(r, "_file_prefix", r._get_file_prefix(tmp=True))

        future_output = r._get_file_prefix(tmp=False)
        if fsspec.filesystem(r._get_protocol(future_output)).exists(future_output):
            raise ValueError(
                f"Output for this artifact already exists in {future_output}"
            )

        return r

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        super().__exit__(exc_type, exc_val, exc_tb)
        object.__setattr__(self, "_file_prefix", None)

    def browser_url(self) -> str:
        if self.main_dir.startswith("gs://"):
            path = self.main_dir.lstrip("gs://")
            return f"https://console.cloud.google.com/storage/browser/{path}"
        else:
            return self.main_dir

    def stage(self, local_path: Path) -> "FSArtifact":
        """
        Dumps local file/directory into a FSArtifact. `local_path` is a file or
        a directory.
        """
        if not local_path.exists():
            raise ValueError(f"`{local_path.as_posix()} does not exists`")
        gcs = fsspec.get_filesystem_class(
            FSArtifact._get_protocol(self.staging_file_prefix)
        )()
        if local_path.is_dir():
            gcs.upload(
                local_path.as_posix(),
                self.staging_file_prefix,
                recursive=True,
            )
        else:
            gcs.upload(local_path.as_posix(), self.joinpath(local_path.name))
        return self
