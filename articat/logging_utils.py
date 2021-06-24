import logging
from typing import Union

import coloredlogs


def setup_console_logging(level: Union[str, int] = logging.INFO) -> None:
    """
    You can use this in the main section of you script to get easy logging.
    :param level: logging level: either a string or logging level, default: "INFO", logging.INFO
    """
    coloredlogs.install(level=level)
