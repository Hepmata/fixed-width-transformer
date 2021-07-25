import sys
from typing import Dict

import pandas as pd
import transformer.result.result_formatter as fmt
from transformer.validator import validator, ValidatorConfig
from transformer.result.result_config import ResultMapperConfig, ResultFormatterConfig
from transformer.library.exceptions import ValidationError, ValidationFailureError

current_module = sys.modules[__name__]


class ResultMapper:
    def run(self, config: ResultMapperConfig, frames: Dict[str, pd.DataFrame]):
        # 1. Run ResultFormatter + Generator
        # 2. Validate
        # Final: Return Result

        data = self._format(config.format, frames)
        self._validate(config.validators, data)
        return data

    def _format(self, config: ResultFormatterConfig, frames) -> Dict[str, pd.DataFrame]:
        data = getattr(fmt, config.name)().run(config, frames)
        return data

    def _validate(self, config: [ValidatorConfig], frames: Dict[str, pd.DataFrame]):
        errors = []
        for segment_validator in config:
            for vld in segment_validator.validators:
                try:
                    getattr(validator, vld.name)().validate(segment_validator.segment, segment_validator.field_name, vld.arguments, frames)
                except ValidationError as v:
                    errors.append(v)

        if len(errors) > 0:
            raise ValidationFailureError("Failed validations for ResultMapper", errors)
