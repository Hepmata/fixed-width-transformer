from dataclasses import dataclass

from transformer.library import exceptions
from transformer.validator import ValidatorConfig, ValidatorFieldConfig
from transformer.converter import ConverterConfig
from transformer.library import logger
import sys


current_module = sys.modules[__name__]

log = logger.set_logger(__name__)


@dataclass
class SourceFormatterConfig:
    name: str
    segment: str
    names: list
    specs: list
    validations: list


@dataclass
class SourceMapperConfig:
    mappers: [SourceFormatterConfig]
    validators: [ValidatorConfig]
    converters: [ConverterConfig]
    trim: bool
    nan_check: bool
    file_name: str

    def __init__(self, config: dict, file_name: str):
        print(config)
        self.mappers = []
        self.validators = []
        self.converters = []
        self.file_name = file_name
        self.trim = True
        self.nan_check = True
        self.configure(config)

    def configure(self, config: dict, file_format="source"):
        mappers = []
        if file_format in config.keys():
            if config[file_format] is None:
                raise exceptions.InvalidConfigError(f"{file_format} segment cannot be empty")
        else:
            raise exceptions.InvalidConfigError(f"{file_format} segment is missing in configuration")
        if 'trim' in config.keys():
            self.trim = config['trim']
        if 'nan_check' in config.keys():
            self.nan_check = config['nan_check']
        for segment in config[file_format]:
            names = []
            specs = []
            validators = []
            for field in config[file_format][segment]['format']:
                names.append(field['name'])
                specs.append(_converter(field['spec']))
                if 'validators' in field.keys():
                    field_validators = []
                    for validator in field['validators']:
                        args = validator['arguments'] if 'arguments' in validator.keys() else None
                        field_validators.append(ValidatorFieldConfig(validator['name'], args))
                    self.validators.append(ValidatorConfig(
                        segment=segment,
                        field_name=field['name'],
                        validators=field_validators
                    ))
                if 'converter' in field.keys():
                    if not isinstance(field['converter'], str):
                        raise exceptions.InvalidConfigError("Field [converter] must be of str type.")
                    self.converters.append(ConverterConfig(
                        segment=segment,
                        field_name=field['name'],
                        name=field['converter']
                    ))
            mappers.append(SourceFormatterConfig(
                name=config[file_format][segment]['formatter'],
                segment=segment,
                names=names,
                specs=specs,
                validations=validators
                )
            )
        self.mappers = mappers

    def get_mappers(self):
        return self.mappers

    def get_converters(self):
        return self.converters

    def get_validators(self):
        return self.validators


def _converter(data: str):
    if not isinstance(data, str):
        raise ValueError("Invalid Type for input [data]")
    if ',' not in data:
        raise ValueError('[data] must be comma seperated! eg. 1,2')
    splits = data.split(',')
    return tuple([int(splits[0].strip()), int(splits[1].strip())])
