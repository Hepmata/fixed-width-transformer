from dataclasses import dataclass

from transformer.library import exceptions
from transformer.validator import ValidatorConfig
from transformer.library import logger
import sys

from transformer.source import source_mapper

current_module = sys.modules[__name__]

log = logger.set_logger(__name__)


@dataclass
class SourceMapperConfig:
    _mappers = [source_mapper.MapperConfig]

    def __init__(self, config: dict):
        self.set_mappers(config)

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

