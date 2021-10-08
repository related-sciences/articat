import logging
import os
import tempfile
import warnings
from configparser import ConfigParser
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, Type, Union

from articat.utils.class_or_instance_method import class_or_instance_method

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from articat.catalog import Catalog


class ArticatMode(str, Enum):
    local = "local"
    gcp_datastore = "gcp_datastore"


class ArticatConfig:
    """
    Articat config handler. Allows to read configuration from config files,
    and config dictionaries. See python's `configparser` for context. This
    class supports "global"/default config and instances of configuration
    which you can supply to Artifact to have custom configured Artifact
    objects, to for example materialise certain Artifacts in custom locations.
    """

    # NOTE: the order in this list matters, and defines the override order
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
        """
        Register configuration from config paths and config dictionary. Config
        paths are read in order, `config_dict` is applied after config paths.
        This methods registers "global"/default configuration, use constructor
        to get an instance of a config. You can also specify the configuration
        location via env variable: ARTICAT_CONFIG.

        `config_paths` resolution order:
         * function argument
         * `ARTICAT_CONFIG` env variable
         * default paths
        """
        if config_paths is None:
            env_config_path = os.environ.get("ARTICAT_CONFIG")
            if env_config_path is not None:
                config_paths = [env_config_path]
            else:
                config_paths = cls.default_config_paths
        cls._config = cls._read_config(
            config_paths=config_paths, config_dict=config_dict
        )
        return cls

    @class_or_instance_method
    def mode(self) -> ArticatMode:
        """
        Articat mode. Currently supported: `local`, `gcp`.
        Defaults to: `local`
        """
        return ArticatMode(self._config.get("main", "mode", fallback=ArticatMode.local))

    @class_or_instance_method
    def catalog(self) -> "Type[Catalog]":
        """Returns the Catalog implementation for given mode"""
        if self.mode() == ArticatMode.local:
            from articat.catalog_local import CatalogLocal

            return CatalogLocal
        elif self.mode() == ArticatMode.gcp_datastore:
            from articat.catalog_datastore import CatalogDatastore

            return CatalogDatastore
        else:
            raise ValueError(f"Unknown catalog for mode: {self.mode}")

    @class_or_instance_method
    def local_db_dir(self) -> str:
        """
        Location of the local DB, `local` mode only.
        Defaults to: ~/.config/articat/local
        """
        return self._config.get(
            "main",
            "local_db_dir",
            fallback=Path.home().joinpath(".config", "articat", "local").as_posix(),
        )

    @class_or_instance_method
    def gcp_project(self) -> str:
        """Google Cloud Platform (GCP) project, for `gcp` mode only"""
        return self._config["gcp"]["project"]

    @class_or_instance_method
    def bq_prod_dataset(self) -> str:
        """BigQuery (BQ) production dataset, for `gcp` mode only"""
        return self._config["bq"]["prod_dataset"]

    @class_or_instance_method
    def bq_tmp_dataset(self) -> str:
        """BigQuery (BQ) temp dataset, for `gcp` mode only"""
        return self._config["bq"]["tmp_dataset"]

    @class_or_instance_method
    def bq_dev_dataset(self) -> str:
        """BigQuery (BQ) development dataset, for `gcp` mode only"""
        return self._config["bq"]["dev_dataset"]

    @classmethod
    def _set_option(
        cls, c: ConfigParser, section: str, option: str, value: Any
    ) -> None:
        if not c.has_section(section):
            c.add_section(section)
        c.set(section, option, value)

    @class_or_instance_method
    def fs_tmp_prefix(self) -> str:
        """File system (FS) temporary/staging location"""
        try:
            return self._config["fs"]["tmp_prefix"]
        except KeyError:
            r = tempfile.mkdtemp(suffix="artifact_temp_dir_")
            self._set_option(self._config, "fs", "tmp_prefix", r)
            warnings.warn(
                f"FSArtifact temp directory not configured, assuming local mode, using temp directory: {r}"
            )
            return r

    @class_or_instance_method
    def fs_dev_prefix(self) -> str:
        """File system (FS) development location"""
        try:
            return self._config["fs"]["dev_prefix"]
        except KeyError:
            r = Path.cwd().joinpath(".articat_catalog", "dev").as_posix()
            self._set_option(self._config, "fs", "dev_prefix", r)
            warnings.warn(
                f"FSArtifact development directory not configured, assuming local mode, using cwd: {r}"
            )
            return r

    @class_or_instance_method
    def fs_prod_prefix(self) -> str:
        """File system (FS) production/final location"""
        try:
            return self._config["fs"]["prod_prefix"]
        except KeyError:
            r = Path.cwd().joinpath(".articat_catalog", "prod").as_posix()
            self._set_option(self._config, "fs", "prod_prefix", r)
            warnings.warn(
                f"FSArtifact production directory not configured, assuming local mode, using cwd: {r}"
            )
            return r


class ConfigMixin:
    """ArticatConfig mixin/trait"""

    _config: Union[Type[ArticatConfig], ArticatConfig]

    @class_or_instance_method
    def config(self) -> Union[Type[ArticatConfig], ArticatConfig]:
        """Get Articat config object"""
        return self._config
