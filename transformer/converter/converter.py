import pandas as pd
from transformer.converter import ConverterConfig
from transformer.library.exceptions import ConversionError


class AbstractConverter:
    def run(self, config: ConverterConfig, series: pd.Series) -> pd.Series: pass


class StrConverter(AbstractConverter):
    def run(self, config: ConverterConfig, series: pd.Series) -> pd.Series:
        return series.astype(str)


class NumberConverter(AbstractConverter):
    def run(self, config: ConverterConfig, series: pd.Series) -> pd.Series:
        try:
            return pd.to_numeric(series)
        except ValueError as e:
            raise ConversionError(msg=str(e), segment=config.segment, field_name=config.field_name)
