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
        formatter = getattr(fmt, config.format.name)()
        prepared_data = formatter.prepare(config.format, frames)
        self._validate(config.validators, prepared_data)
        return formatter.transform(prepared_data)

    def _validate(self, config: [ValidatorConfig], frames: Dict[str, pd.DataFrame]):
        errors = []
        for cfg in config:
            for vld in cfg.validators:
                try:
                    getattr(validator, vld.name)().validate(
                        cfg.segment, cfg.field_name, vld.arguments, frames
                    )
                except ValidationError as v:
                    errors.append(v)

        if len(errors) > 0:
            raise ValidationFailureError('Failed validations for ResultMapper', errors)
