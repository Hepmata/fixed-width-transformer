import sys
import pandas as pd
import transformer.result.result_formatter as fmt
from transformer.validator import validator
from transformer.result.result_config import ResultMapperConfig, ResultFormatterConfig
from transformer.library.exceptions import ValidationError, ValidationFailureError

current_module = sys.modules[__name__]


class ResultMapper:
    config: ResultMapperConfig

    def __init__(self, config: ResultMapperConfig):
        self.config = config

    def run(self, frames: dict[str, pd.DataFrame]):
        # 1. Run ResultFormatter + Generator
        data = self._format(self.config.segment_format, frames)
        # 2. Run Converter
        # self._convert()
        # 3. Run Validations
        # self._validate(self.config.segment_format, data)
        # Final: Return Result

        return data

    def _format(self, config: ResultFormatterConfig, frames):
        data = getattr(fmt, config.mapper)().run(config, frames)
        return data

    def _convert(self):
        pass

    def _validate(self, config: ResultFormatterConfig, frames: dict[str, pd.DataFrame]):
        errors = []
        for segment_validator in config.segment_validators:
            for vld in segment_validator.validators:
                try:
                    getattr(validator, vld.name)().validate(segment_validator.segment, segment_validator.field_name, vld.arguments, frames)
                except ValidationError as v:
                    errors.append(v)

        if len(errors) > 0:
            raise ValidationFailureError("Failed validations for ResultMapper", errors)