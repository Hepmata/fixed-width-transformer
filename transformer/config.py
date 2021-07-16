import dataclasses
import os
import re
import sys
import yaml
from library import common, exceptions, logger, aws_service
from transformer import source_mapper

log = logger.set_logger(__name__)

current_module = sys.modules[__name__]


class ExecutorConfig:
    _config = dict
    _exact_config = dict

    def __init__(self, key: str):
        try:
            self._config = self._retrieve_config()
            self._set_exact_config(key)
        except Exception as e:
            print(e)
            raise e

    def _retrieve_config(self):
        if os.environ['config_type'] == "external":
            required_configs = ["config_external_bucket", "config_name"]
            env_config = common.check_environment_variables(required_configs)
            log.info(f"Using External Configuration file from S3 bucket [{env_config[0]}] with key [{env_config[1]}")
            return yaml.load(
                aws_service.download_s3_as_bytes(env_config[0], env_config[1]).read(),
                Loader=yaml.FullLoader
            )
        else:
            env_config = common.check_environment_variables(["config_name"])
            log.info(f"Using Local Configuration file [{env_config[0]}]")
            with open(env_config[0], 'r') as file:
                return yaml.load(file, Loader=yaml.FullLoader)

    def _set_exact_config(self, key):
        for k in self._config['files']:
            pattern = self._config['files'][k]['pattern']
            if re.match(pattern, key):
                self._exact_config = self._config['files'][k]
                return
        raise exceptions.MissingConfigError(
            f"No matching regex pattern found for file with name [{key}]. Please check configuration yaml file")

    def get_config(self):
        return self._config

    def get_exact_config(self):
        return self._exact_config


@dataclasses.dataclass
class ResultMapperConfig:
    _result_config = dict
    _validations = [dict]

    def __init__(self, config: ExecutorConfig):
        self.set_result_config(config.get_exact_config())

    def set_result_config(self, config):
        # TODO: Enhance if need to formalize the config in a better flow.
        print(config)
        if 'format' in config['output']:
            self._result_config = config['output']['format']
        else:
            self._result_config = {}

    def get_result_config(self):
        return self._result_config


@dataclasses.dataclass
class SourceMapperConfig:
    _validations: [dict]
    _mappers = [source_mapper.AbstractDataMapper]

    def __init__(self, config: ExecutorConfig):
        self.set_mappers(config.get_exact_config())
        pass

    def set_mappers(self, config: dict, file_format="fileFormat"):
        mapping = {}
        mappers = []
        for segment in config[file_format]:
            name = []
            specs = []
            for field in config[file_format][segment]['format']:
                name.append(field['name'])
                specs.append(self._converter(field['spec']))
            mapping[segment] = {
                'names': name,
                'specs': specs
            }
            mappers.append(getattr(source_mapper, config[file_format][segment]['mapper'])(mapping[segment]))
        self._mappers = mappers
        print(len(self._mappers))

    def _converter(self, data: str):
        if not isinstance(data, str):
            raise ValueError("Invalid Type for input [data]")
        if ',' not in data:
            raise ValueError('[data] must be comma seperated! eg. 1,2')
        splits = data.split(',')
        return tuple([int(splits[0].strip()), int(splits[1].strip())])

    def set_validations(self):
        pass

    def get_validations(self):
        pass

    def get_mappers(self):
        return self._mappers


@dataclasses.dataclass
class ResultConfig:
    _name: str
    _arguments: dict

    def __init__(self, config: ExecutorConfig):
        self._name = config.get_exact_config()['output']['result']['name']
        self._arguments = {}
        if 'arguments' in config.get_exact_config()['output']['result'].keys():
            self._arguments = config.get_exact_config()['output']['result']['arguments']

    def get_name(self) -> str:
        return self._name

    def get_arguments(self) -> dict:
        return self._arguments
