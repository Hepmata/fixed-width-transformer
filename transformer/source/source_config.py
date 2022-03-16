from dataclasses import dataclass

import yaml

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


@dataclass
class ConstraintConfig:
    name: str
    arguments: dict


@dataclass
class SourceMapperConfig:
    mappers: [SourceFormatterConfig]
    validators: [ValidatorConfig]
    converters: [ConverterConfig]
    constraints: [ConstraintConfig]
    trim: bool
    nan_check: bool
    file_name: str

    def __init__(self, config: dict, file_name: str):
        print(config)
        self.mappers = []
        self.validators = []
        self.converters = []
        self.constraints = []
        self.file_name = file_name
        self.trim = True
        self.nan_check = True
        self.configure(config)

    def configure(self, config: dict, file_format='source'):
        mappers = []
        if file_format in config.keys():
            if config[file_format] is None:
                raise exceptions.InvalidConfigError(
                    f'{file_format} segment cannot be empty'
                )
        else:
            raise exceptions.InvalidConfigError(
                f'{file_format} segment is missing in configuration'
            )
        if 'trim' in config.keys():
            self.trim = config['trim']
        if 'nan_check' in config.keys():
            self.nan_check = config['nan_check']
        if 'constraints' in config.keys():
            def constraint_builder(csrt):
                return ConstraintConfig(
                    name=csrt['name'],
                    arguments=csrt['arguments']
                )
            if type(config['constraints']) == dict:
                self.constraints.append(config['constraints'])
            else:
                for constraint in config['constraints']:
                    self.constraints.append(constraint_builder(constraint))

        if 'include' in config[file_format]:
            with open(config[file_format]['include']) as file:
                content = yaml.safe_load(file)
            config[file_format].pop('include')
            config[file_format] = content
        for segment in config[file_format]:
            names = []
            specs = []
            for field in config[file_format][segment]['format']:
                names.append(field['name'])
                if 'spec' in field.keys():
                    specs.append(_converter(field['spec']))
                if 'validators' in field.keys():
                    field_validators = []
                    for validator in field['validators']:
                        args = (
                            validator['arguments']
                            if 'arguments' in validator.keys()
                            else {}
                        )
                        field_validators.append(
                            ValidatorFieldConfig(validator['name'], args)
                        )
                    self.validators.append(
                        ValidatorConfig(
                            segment=segment,
                            field_name=field['name'],
                            validators=field_validators,
                        )
                    )
                if 'converter' in field.keys():
                    if not isinstance(field['converter'], str):
                        raise exceptions.InvalidConfigError(
                            'Field [converter] must be of str type.'
                        )
                    self.converters.append(
                        ConverterConfig(
                            segment=segment,
                            field_name=field['name'],
                            name=field['converter'],
                        )
                    )
            mappers.append(
                SourceFormatterConfig(
                    name=config[file_format][segment]['formatter'],
                    segment=segment,
                    names=names,
                    specs=specs,
                )
            )
        self.mappers = mappers

    def get_mappers(self):
        return self.mappers

    def get_converters(self):
        return self.converters

    def get_validators(self):
        return self.validators

    def get_constraints(self):
        return self.constraints

    def get_validation_array(self):
        validations = []
        for v in self.validators:
            for field_validator in v.validators:
                validations.append(
                    {
                        'type': 'Result',
                        'segment': v.segment,
                        'field': v.field_name,
                        'validator': field_validator.name,
                    }
                )
        return validations

    def get_constraint_array(self):
        constraints = []
        for c in self.constraints:
            constraints.append({
                'name': c.name,
                'arguments': c.arguments
            })
        return constraints


def _converter(data: str):
    if not isinstance(data, str):
        raise ValueError('Invalid Type for input [data]')
    if ',' not in data:
        raise ValueError('[data] must be comma seperated! eg. 1,2')
    splits = data.split(',')
    return tuple([int(splits[0].strip()), int(splits[1].strip())])
