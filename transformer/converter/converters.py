import pandas as pd
from transformer.converter import ConverterConfig
from transformer.library.exceptions import ConversionError


class AbstractConverter:
    config: ConverterConfig

    def __init__(self, config: ConverterConfig):
        self.config = config

    def run(self, frames: dict[str, pd.DataFrame]) -> pd.Series: pass


class StrConverter(AbstractConverter):
    def __init__(self, config: ConverterConfig):
        super().__init__(config)

    def run(self, frames: dict[str, pd.DataFrame]) -> pd.Series:
        return frames[self.config.segment][self.config.fieldName].astype(str)


class NumberConverter(AbstractConverter):
    def __init__(self, config: ConverterConfig):
        super().__init__(config)

    def run(self, frames: dict[str, pd.DataFrame]) -> pd.Series:
        try:
            return pd.to_numeric(frames[self.config.segment][self.config.fieldName])
        except ValueError as e:
            raise ConversionError(msg=e, segment=self.config.segment, field_name=self.config.fieldName)