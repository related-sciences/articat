import shutil
from pathlib import Path
from typing import Optional

import fsspec

from articat.utils.typing import PathType


def to_pathlib(path: PathType) -> Path:
    """Coerce to pathlib Path"""
    if isinstance(path, str):
        return Path(path)
    else:
        return path


def get_root_path() -> Path:
    """Return root of the articat project"""
    return Path(__file__).parent.parent.parent.absolute()


def fsspec_copyfile(
    src: str,
    dst: str,
    length: int = 0,
    src_compression: Optional[str] = None,
    dst_compression: Optional[str] = None,
) -> None:
    """
    Like `shutil.copyfile` but with support for `fsspec` paths
    and compression.
    """
    with fsspec.open(src, mode="rb", compression=src_compression) as fsrc, fsspec.open(
        dst, mode="wb", compression=dst_compression
    ) as fdst:
        shutil.copyfileobj(fsrc, fdst, length=length)
