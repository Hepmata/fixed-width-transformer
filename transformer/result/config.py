import dataclasses
from transformer.library.exceptions import InvalidConfigError
from transformer.validator.validator_config import ValidatorConfig, ValidatorFieldConfig


@dataclasses.dataclass
class ResultFieldFormat:
    name: str
    value: str


@dataclasses.dataclass
class ResultFormatterConfig:
    mapper: str
    segment_formats: dict[str, list[ResultFieldFormat]]
    segment_validators: [ValidatorConfig]

    def __init__(self, config: dict):
        fmt = "format"
        self.mapper = config['mapper']
        self.segment_formats = {}
        self.segment_validators = []
        if fmt not in config.keys():
            return
        for segment in config[fmt]:
            segment_format = []
            for field in config[fmt][segment]:
                segment_format.append(
                    ResultFieldFormat(
                        name=field['name'],
                        value=field['value']
                    )
                )
                if 'validators' in field.keys():
                    validators = []
                    for validator in field['validators']:
                        validators.append(
                            ValidatorFieldConfig(
                                name=validator['name'],
                                arguments=validator['arguments']
                            )
                        )
                    self.segment_validators.append(
                        ValidatorConfig(
                            segment=segment,
                            field_name=field['name'],
                            validations=validators
                        )
                    )
            self.segment_formats[segment] = segment_format


@dataclasses.dataclass
class ResultMapperConfig:
    _result_config: dict
    segment_format: ResultFormatterConfig

    def __init__(self, config: dict):
        self.set_result_config(config)
        self.segment_format = ResultFormatterConfig(self._result_config)

    def set_result_config(self, config):
        try:
            if 'output' in config:
                self._result_config = config['output']
            else:
                self._result_config = {}
        except KeyError as e:
            raise InvalidConfigError(e)

    def get_result_config(self):
        return self._result_config
