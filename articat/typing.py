from pathlib import Path
from typing import TYPE_CHECKING, TypeVar, Union

import pandas
import pydantic

if TYPE_CHECKING:
    import pyspark

PathType = Union[str, Path]

PandasDataFrame = pandas.DataFrame

# NOTE: because right now PandasDataFrame is untyped,
#       it effectively makes DataFrame Any :/
DataFrame = Union["pyspark.sql.DataFrame", pandas.DataFrame]
"""Represents known DataFrames"""
PYDANTIC_MODEL = TypeVar("PYDANTIC_MODEL", bound=pydantic.BaseModel)
