import dataclasses
from typing import Dict, List

import yaml

from transformer.library.exceptions import InvalidConfigError
from transformer.validator.validator_config import ValidatorConfig, ValidatorFieldConfig


@dataclasses.dataclass
class ResultFieldFormat:
    name: str
    value: str


@dataclasses.dataclass
class ResultFormatterConfig:
    name: str
    formats: Dict[str, List[ResultFieldFormat]]


@dataclasses.dataclass
class ResultMapperConfig:
    format: ResultFormatterConfig
    validators: [ValidatorConfig]

    def __init__(self, config: dict):
        self.validators = []
        self.configure(config)

    def configure(self, config: dict):
        rst = 'result'
        fmt = 'format'
        formats = {}

        if fmt not in config[rst].keys():
            self.format = ResultFormatterConfig(
                name=config[rst]['formatter'], formats={}
            )
            return
        if not config[rst][fmt]:
            self.format = ResultFormatterConfig(
                name=config[rst]['formatter'], formats={}
            )
            return
        if 'include' in config[rst][fmt].keys():
            with open(config[rst][fmt]['include']) as file:
                content = yaml.safe_load(file)
            config[rst][fmt].pop('include')
            config[rst][fmt] = content
        for segment in config[rst][fmt]:
            format = []
            for field in config[rst][fmt][segment]:
                format.append(
                    ResultFieldFormat(name=field['name'], value=field['value'])
                )
                if 'validators' in field.keys():
                    validators = []
                    for validator in field['validators']:
                        args = (
                            validator['arguments']
                            if 'arguments' in validator.keys()
                            else {}
                        )
                        validators.append(
                            ValidatorFieldConfig(name=validator['name'], arguments=args)
                        )
                    self.validators.append(
                        ValidatorConfig(
                            segment=segment,
                            field_name=field['name'],
                            validators=validators,
                        )
                    )
            formats[segment] = format
        self.format = ResultFormatterConfig(
            name=config[rst]['formatter'], formats=formats
        )

    def get_validation_array(self):
        validations = []
        for v in self.validators:
            for field_validator in v.validators:
                validations.append(
                    {
                        'type': 'Source',
                        'segment': v.segment,
                        'field': v.field_name,
                        'validator': field_validator.name,
                    }
                )
        return validations


@dataclasses.dataclass
class ResultProducer:
    name: str
    arguments: dict


@dataclasses.dataclass
class ResultProducerConfig:
    producers: List[ResultProducer]

    def __init__(self, config: dict):
        if isinstance(config['result']['producer'], dict):
            self.producers = [
                ResultProducer(
                    name=config['result']['producer']['name'],
                    arguments=config['result']['producer']['arguments']
                    if 'arguments' in config['result']['producer']
                    else {},
                )
            ]
        elif isinstance(config['result']['producer'], list):
            self.producers = []
            for p in config['result']['producer']:
                self.producers.append(
                    ResultProducer(
                        name=p['name'],
                        arguments=p['arguments'] if 'arguments' in p.keys() else {},
                    )
                )
        else:
            raise InvalidConfigError(
                'Producer must be a dictionary with keys [name, arguments] or an array with said keys'
            )
