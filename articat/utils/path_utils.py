import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import fsspec

from articat.utils.typing import PathType


def to_pathlib(path: PathType) -> Path:
    """Coerce to pathlib Path"""
    if isinstance(path, str):
        return Path(path)
    else:
        return path


def fsspec_copyfile(
    src: str,
    dst: str,
    length: int = 0,
    src_compression: str | None = None,
    dst_compression: str | None = None,
) -> None:
    """
    Like `shutil.copyfile` but with support for `fsspec` paths
    and compression.
    """
    with fsspec.open(src, mode="rb", compression=src_compression) as fsrc, fsspec.open(
        dst, mode="wb", compression=dst_compression
    ) as fdst:
        shutil.copyfileobj(fsrc, fdst, length=length)


@contextmanager
def cwd(new_cwd: str | Path) -> Iterator[None]:
    """
    A context manager which changes the working directory to the given
    path, and then changes it back to its previous value on exit.

    Credit: https://gist.github.com/nottrobin/3d675653244f8814838a
    """

    prev_cwd = os.getcwd()
    try:
        os.chdir(new_cwd)
        yield
    finally:
        os.chdir(prev_cwd)
