from transformer.library.exceptions import InvalidConfigError, MissingConfigError
from transformer.library import common, aws_service, logger
import re
import yaml
import os


log = logger.set_logger(__name__)


class ExecutorConfig:
    _config = dict
    _exact_config = dict

    def __init__(self, key: str, local=None, inline=None):
        try:
            self._config = self._retrieve_config(local, inline)
            self._set_exact_config(key)
        except Exception as e:
            print(e)
            raise e

    def _retrieve_config(self, local=None, inline=None) -> dict:
        if inline:
            cfg = yaml.safe_load(inline)
            if isinstance(cfg, dict):
                return cfg
            raise InvalidConfigError()
        if local is None:
            # To retrieve config using environment variables
            if os.environ['config_type'] == "local":
                env_config = common.check_environment_variables(["config_name"])
                log.info(f"Using Local Configuration file [{env_config[0]}]")
                with open(env_config[0], 'r') as file:
                    cfg = yaml.safe_load(file)
                    if isinstance(cfg, dict):
                        return cfg
                    raise InvalidConfigError()

            # Retrieve S3 remote config
            required_configs = ["config_bucket", "config_name"]
            env_config = common.check_environment_variables(required_configs)
            log.info(f"Using External Configuration file from S3 bucket [{env_config[0]}] with key [{env_config[1]}")
            cfg = yaml.safe_load(
                aws_service.download_s3_as_bytes(env_config[0], env_config[1]).read()
            )
            if isinstance(cfg, dict):
                return cfg
            raise InvalidConfigError()
        else:
            log.info(f"Using Local Configuration file [{local}]")
            try:
                with open(local, 'r') as file:
                    cfg = yaml.safe_load(file)
                    if isinstance(cfg, dict):
                        return cfg
                    raise
            except FileNotFoundError as e:
                raise MissingConfigError(e)

    def _set_exact_config(self, key):
        if self._config['files'] is None:
            raise InvalidConfigError()
        for k in self._config['files']:
            pattern = self._config['files'][k]['pattern']
            if re.match(pattern, key):
                self._exact_config = self._config['files'][k]
                return
        raise MissingConfigError(
            f"No matching regex pattern found for file with name [{key}]. Please check configuration yaml file")

    def get_config(self):
        return self._config

    def get_exact_config(self):
        return self._exact_config
