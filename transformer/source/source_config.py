from dataclasses import dataclass
from transformer.library import logger
import sys

current_module = sys.modules[__name__]

log = logger.set_logger(__name__)


@dataclass
class SourceMapperConfig:
    _mappers = [source_mapper.MapperConfig]

    def __init__(self, config: ExecutorConfig):
        self.set_mappers(config.get_exact_config())

    def set_mappers(self, config: dict, file_format="source"):
        mappers = []
        if file_format in config.keys():
            if config[file_format] is None:
                raise exceptions.InvalidConfigError(f"{file_format} segment cannot be empty")
        else:
            raise exceptions.InvalidConfigError(f"{file_format} segment is missing in configuration")

        has_header = True if 'header' in config[file_format].keys() else False
        has_footer = True if 'footer' in config[file_format].keys() else False
        for segment in config[file_format]:
            names = []
            specs = []
            validators = []
            for field in config[file_format][segment]['format']:
                names.append(field['name'])
                specs.append(self._converter(field['spec']))
                if 'validators' in field.keys():
                    for validator in field['validators']:
                        validators.append(ValidatorConfig(validator['name'], segment, field['name'], validator['arguments']))
            mappers.append(source_mapper.MapperConfig(
                name=config[file_format][segment]['mapper'],
                segment=segment,
                names=names,
                specs=specs,
                skipHeader=has_header,
                skipFooter=has_footer,
                validations=validators
                )
            )
        self._mappers = mappers

    def _converter(self, data: str):
        if not isinstance(data, str):
            raise ValueError("Invalid Type for input [data]")
        if ',' not in data:
            raise ValueError('[data] must be comma seperated! eg. 1,2')
        splits = data.split(',')
        return tuple([int(splits[0].strip()), int(splits[1].strip())])

    def get_mappers(self):
        return self._mappers



@dataclasses.dataclass
class ResultConfig:
    _name: str
    _arguments: dict

    def __init__(self, config: ExecutorConfig):
        if 'result' not in config.get_exact_config()['output'].keys():
            raise exceptions.InvalidConfigError('result segment is missing. Please ensure configuration is provided')
        if config.get_exact_config()['output']['result'] is None or not config.get_exact_config()['output']['result']:
            raise exceptions.InvalidConfigError('result segment cannot be empty. Please ensure configuration is valid')

        self._name = config.get_exact_config()['output']['result']['name']
        self._arguments = {}
        if 'arguments' in config.get_exact_config()['output']['result'].keys():
            self._arguments = config.get_exact_config()['output']['result']['arguments']

    def get_name(self) -> str:
        return self._name

    def get_arguments(self) -> dict:
        return self._arguments


