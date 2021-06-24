import io
import logging
from collections.abc import Sequence
from typing import Any

import numpy as np
from pandas import DataFrame, Series

logger = logging.getLogger(__name__)


def get_df_info(df: DataFrame) -> str:
    """Get DataFrame info as string"""
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()


def stringify_lists(x: Any, sep: str = " | ") -> Any:
    """
    Convert lists/tuples/sets to strings by joining with `sep`.
    Can be applied to an entire pandas.DataFrame with `.applymap(stringify_lists)`.
    """
    if isinstance(x, str):
        return x
    if isinstance(x, set):
        x = sorted(x)
    if not isinstance(x, (Sequence, np.ndarray, Series)):
        return x
    return sep.join(map(str, x))
