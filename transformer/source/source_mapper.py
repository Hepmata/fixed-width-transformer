from transformer.library import logger
from transformer.library.exceptions import SourceFileError
from transformer.decorators import PreValidate
from transformer.source.source_config import SourceMapperConfig
from transformer.source.source_config import SourceFormatterConfig
from transformer.source import source_formatter
from transformer.converter import ConverterConfig
from transformer.validator import ValidatorConfig
import dataclasses
from io import StringIO
import pandas as pd

log = logger.set_logger(__name__)


class SourceMapper:
    def run(self, config: SourceMapperConfig) -> dict[str, pd.DataFrame]:
        """
        The execution of the above steps are as follows:
        1. SourceFormatter to convert data from File to DataFrames
        2. A NaN validation is then applied by default. To prevent this behaviour, provide an override in the config
        3. Custom Validations are then executed if provided. Else this section will be skipped
        4. Default Converter is then executed to trim away all whitespaces in DataFrames. To prevent this behaviour, provide and override in the config
        """
        dataframes = self._format(config.get_mappers())
        return dataframes

    def _format(self, config: [SourceFormatterConfig]) -> dict[str, pd.DataFrame]:
        dataframes = {}
        for cfg in config:
            dataframes[cfg['segment']] = getattr(source_formatter, cfg['name'])().run(cfg)
        return dataframes

    def _convert(self, config: [ConverterConfig]) -> dict[str, pd.DataFrame]:
        pass

    def _validate(self, config: [ValidatorConfig]) -> None:
        pass
