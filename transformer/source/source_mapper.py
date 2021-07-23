from transformer.library import logger
from transformer.library.exceptions import SourceFileError
from transformer.decorators import PreValidate
from transformer.source.source_config import SourceMapperConfig
import dataclasses
from io import StringIO
import pandas as pd

log = logger.set_logger(__name__)


class SourceMapper:
    def run(self, config: SourceMapperConfig):
        """
        The execution of the above steps are as follows:
        1. SourceFormatter to convert data from File to DataFrames
        2. A NaN validation is then applied by default. To prevent this behaviour, provide an override in the config
        3. Custom Validations are then executed if provided. Else this section will be skipped
        4. Default Converter is then executed to trim away all whitespaces in DataFrames. To prevent this behaviour, provide and override in the config
        """
