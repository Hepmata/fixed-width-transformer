import dataclasses
from typing import Dict, List

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
        print(config)
        rst = "result"
        fmt = "format"
        formats = {}
        if fmt not in config[rst].keys():
            return

        for segment in config[rst][fmt]:
            format = []
            for field in config[rst][fmt][segment]:
                format.append(
                    ResultFieldFormat(
                        name=field['name'],
                        value=field['value']
                    )
                )
                if 'validators' in field.keys():
                    validators = []
                    for validator in field['validators']:
                        args = validator['arguments'] if 'arguments' in validator.keys() else {}
                        validators.append(
                            ValidatorFieldConfig(
                                name=validator['name'],
                                arguments=args
                            )
                        )
                    self.validators = validators
            formats[segment] = format
        self.format = ResultFormatterConfig(name=config[rst]['formatter'], formats=formats)


@dataclasses.dataclass
class ResultProducerConfig:
    name: str
    arguments: dict

    def __init__(self, config: dict):
        self.name = config['result']['producer']['name']
        args = config['result']['producer']['arguments'] if 'arguments' in config['result']['producer'] else {}
        self.arguments = args
