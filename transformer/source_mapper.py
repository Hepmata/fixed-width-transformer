from transformer.library import logger
from transformer.library.exceptions import SourceFileError
from transformer.decorators import PreValidate
import dataclasses
from io import StringIO
import pandas as pd

log = logger.set_logger(__name__)


@dataclasses.dataclass
class MapperConfig:
    name: str
    segment: str
    names: list
    specs: list
    skipHeader: bool
    skipFooter: bool
    validations: list


class AbstractDataMapper:
    def run(self, config: MapperConfig, file_name: str) -> pd.DataFrame: pass


class HeaderDataMapper(AbstractDataMapper):
    def run(self, config: MapperConfig, file_name: str) -> pd.DataFrame:
        try:
            data = pd.read_fwf(file_name, colspecs=config.specs, names=config.names,
                               converters={h: str for h in config.names}, delimiter="\n\t", nrows=1)
            if len(data.index) == 0:
                raise SourceFileError("Invalid Source File, Index is empty", file_name)
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class BodyDataMapper(AbstractDataMapper):
    @PreValidate
    def run(self, config: MapperConfig, file_name: str) -> pd.DataFrame:
        header = 0 if config.skipHeader else None
        footer = 1 if config.skipFooter else 0
        try:
            data = pd.read_fwf(file_name, colspecs=config.specs, header=header,
                               names=config.names,
                               converters={h: str for h in config.names}, skipfooter=footer,
                               delimiter="\n\t")
            if len(data.index) == 0:
                raise SourceFileError("Invalid Source File, Index is empty", file_name)
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class FooterDataMapper(AbstractDataMapper):
    def run(self, config: MapperConfig, file_name: str) -> pd.DataFrame:
        try:
            with open(file_name, 'r') as lines:
                read = lines.readlines()
                if len(read) == 0:
                    raise SourceFileError("Invalid Source File, Index is empty", file_name)
                last_line = read[-1]
                data = pd.read_fwf(StringIO(last_line), colspecs=config.specs,
                                   names=config.names,
                                   converters={h: str for h in config.names}, delimiter="\n\t")
                return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)
