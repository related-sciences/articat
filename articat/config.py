import logging
from configparser import ConfigParser
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Type, Union

from articat.utils.classproperty import classproperty

logger = logging.getLogger(__name__)


class ArticatConfig:
    default_config_paths = [
        Path.home().joinpath(".config", "articat", "articat.cfg").as_posix(),
        Path.cwd().joinpath("articat.cfg").as_posix(),
    ]
    _config: ConfigParser = ConfigParser()

    def __init__(
        self,
        config_paths: Sequence[str] = [],
        config_dict: Mapping[str, Mapping[str, Any]] = {},
    ) -> None:
        self._config = self._read_config(
            config_paths=config_paths, config_dict=config_dict
        )

    @staticmethod
    def _read_config(
        config_paths: Sequence[str], config_dict: Mapping[str, Mapping[str, Any]]
    ) -> ConfigParser:
        config = ConfigParser()
        read = config.read(config_paths)
        if len(read) == 0:
            logger.warning(f"No configuration files found, searched in {config_paths}")
        config.read_dict(config_dict)
        return config

    @classmethod
    def register_config(
        cls,
        config_paths: Optional[Sequence[str]] = None,
        config_dict: Mapping[str, Mapping[str, Any]] = {},
    ) -> "Type[ArticatConfig]":
        """TODO"""
        config_paths = (
            config_paths if config_paths is not None else cls.default_config_paths
        )
        cls._config = cls._read_config(
            config_paths=config_paths, config_dict=config_dict
        )
        return cls

    @classproperty
    def gcp_project(self) -> str:
        return self._config["gcp"]["project"]

    @classproperty
    def bq_prod_dataset(self) -> str:
        return self._config["bq"]["prod_dataset"]

    @classproperty
    def bq_dev_dataset(self) -> str:
        return self._config["bq"]["dev_dataset"]

    @classproperty
    def fs_tmp_prefix(self) -> str:
        return self._config["fs"]["tmp_prefix"]

    @classproperty
    def fs_dev_prefix(self) -> str:
        return self._config["fs"]["dev_prefix"]

    @classproperty
    def fs_prod_prefix(self) -> str:
        return self._config["fs"]["prod_prefix"]


class ConfigMixin:
    _config: Union[Type[ArticatConfig], ArticatConfig]

    @classproperty
    def config(self) -> Union[Type[ArticatConfig], ArticatConfig]:
        """Get Articat config object"""
        return self._config
