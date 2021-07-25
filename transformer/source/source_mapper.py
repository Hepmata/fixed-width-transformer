from typing import Dict

from transformer.library import logger
from transformer.source.source_config import SourceMapperConfig
from transformer.source.source_config import SourceFormatterConfig
from transformer.source import source_formatter
from transformer.converter import ConverterConfig, converter
from transformer.validator import ValidatorConfig, validator
from transformer.library.exceptions import ValidationError, ValidationFailureError
import pandas as pd

log = logger.set_logger(__name__)


class SourceMapper:
    def run(self, config: SourceMapperConfig) -> Dict[str, pd.DataFrame]:
        """
        The execution of the above steps are as follows:
        1. SourceFormatter to convert data from File to DataFrames
        2. A NaN validation is then applied by default. To prevent this behaviour, provide an override in the config
        3. Custom Validations are then executed if provided. Else this section will be skipped
        4. Default Converter is then executed to trim away all whitespaces in DataFrames. To prevent this behaviour, provide and override in the config
        """
        dataframes = self._format(config.get_mappers(), config.file_name)
        if config.nan_check:
            errors = []
            for df in dataframes:
                try:
                    validator.NaNValidator().validate(segment=df, field_name="ALL", arguments={}, frames=dataframes)
                except ValidationError as e:
                    errors.append(e)
            if len(errors) > 0:
                raise ValidationFailureError(f"Nan Validation failed for {len(errors)} segments.", errors)
        self._validate(config.get_validators(), dataframes)
        if config.trim:
            dataframes = self._trim(dataframes)
        dataframes = self._convert(config.get_converters(), dataframes)
        return dataframes

    def _format(self, config: [SourceFormatterConfig], file_name) -> Dict[str, pd.DataFrame]:
        dataframes = {}
        for cfg in config:
            dataframes[cfg.segment] = getattr(source_formatter, cfg.name)().run(cfg, file_name)
        return dataframes

    def _convert(self, config: [ConverterConfig], dataframes: [str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        for cfg in config:
            dataframes[cfg.segment][cfg.field_name] = getattr(converter, cfg.name)().run(cfg, dataframes[cfg.segment][cfg.field_name])
        return dataframes

    def _validate(self, config: [ValidatorConfig], dataframes: [str, pd.DataFrame]) -> None:
        errors = []
        for cfg in config:
            for vld in cfg.validators:
                try:
                    getattr(validator, vld.name)().validate(
                        segment=cfg.segment,
                        field_name = cfg.field_name,
                        arguments=vld.arguments,
                        frames=dataframes
                    )
                except ValidationError as e:
                    print(e)
                    errors.append(e)

        if len(errors) > 0:
            raise ValidationFailureError(f"There are {len(errors)} pre-validation errors. {errors}", errors)

    def _trim(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        for df in dataframes:
            dataframes[df] = dataframes[df].applymap(lambda x: x.strip() if isinstance(x, str) else x)

        return dataframes
