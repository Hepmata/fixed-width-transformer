from config import ResultMapperConfig
import sys
import pandas as pd
import transformer.result.generator as generator
from config import ResultMapperConfig, ResultFormatterConfig

current_module = sys.modules[__name__]


class ResultMapper:
    config: ResultMapperConfig

    def __init__(self, config: ResultMapperConfig):
        self.config = config

    def run(self):
        # 1. Run ResultFormatter + Generator

        self._format(self.config.segment_format)
        # 2. Run Converter
        self._convert()
        # 3. Run Validations
        self._validate()
        # Final: Return Result
        pass

    def _format(self, config: ResultFormatterConfig, frames):
        pass

    def _convert(self):
        pass

    def _validate(self):
        pass
