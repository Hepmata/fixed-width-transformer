from transformer.library import logger
from transformer.library.exceptions import SourceFileError
from transformer.source import SourceFormatterConfig
from io import StringIO
import pandas as pd

log = logger.set_logger(__name__)


class AbstractDataMapper:
    def run(self, config: SourceFormatterConfig, file_name: str) -> pd.DataFrame:
        pass


class CSVHeaderFormatter(AbstractDataMapper):
    def run(self, config: SourceFormatterConfig, file_name: str) -> pd.DataFrame:
        try:
            data = pd.read_csv(
                file_name,
                header=None,
                names=config.names,
                converters={h: str for h in config.names},
                delimiter=',',
                nrows=1,
            )
            if len(data.index) == 0:
                raise SourceFileError('Invalid Source File, Index is empty', file_name)
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class CSVBodyFormatter(AbstractDataMapper):
    def run(self, config: SourceFormatterConfig, file_name: str) -> pd.DataFrame:
        try:
            data = pd.read_csv(
                file_name,
                header=None,
                names=config.names,
                usecols=config.names,
                skiprows=1,
                engine='python',
                converters={h: str for h in config.names},
                skipfooter=1,
                delimiter=',',
            )
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class CSVBodyNoFooterFormatter(AbstractDataMapper):
    def run(self, config: SourceFormatterConfig, file_name: str) -> pd.DataFrame:
        try:
            data = pd.read_csv(
                file_name,
                header=None,
                names=config.names,
                usecols=config.names,
                skiprows=1,
                engine='python',
                converters={h: str for h in config.names},
                delimiter=',',
            )
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class CSVTrailerFormatter(AbstractDataMapper):
    def run(self, config: SourceFormatterConfig, file_name: str) -> pd.DataFrame:
        try:
            with open(file_name, 'r') as f:
                lines = f.read()
                read = lines.splitlines()
                if len(read) == 0:
                    raise SourceFileError(
                        'Invalid Source File, Index is empty', file_name
                    )
                last_line = read[-1]
            data = pd.read_csv(
                StringIO(last_line),
                header=None,
                names=config.names,
                usecols=config.names,
                converters={h: str for h in config.names},
                delimiter=',',
            )
            if len(data.index) == 0:
                raise SourceFileError('Invalid Source File, Index is empty', file_name)
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class HeaderSourceFormatter(AbstractDataMapper):
    def run(self, config: SourceFormatterConfig, file_name: str) -> pd.DataFrame:
        try:
            data = pd.read_fwf(
                file_name,
                colspecs=config.specs,
                names=config.names,
                converters={h: str for h in config.names},
                delimiter='\n\t',
                nrows=1,
            )
            if len(data.index) == 0:
                raise SourceFileError('Invalid Source File, Index is empty', file_name)
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class BodySourceFormatter(AbstractDataMapper):
    def run(self, config: SourceFormatterConfig, file_name: str) -> pd.DataFrame:
        try:
            data = pd.read_fwf(
                file_name,
                colspecs=config.specs,
                header=0,
                names=config.names,
                converters={h: str for h in config.names},
                skipfooter=1,
                delimiter='\n\t',
            )
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class FooterSourceFormatter(AbstractDataMapper):
    def run(self, config: SourceFormatterConfig, file_name: str) -> pd.DataFrame:
        try:
            with open(file_name, 'r') as f:
                lines = f.read()
                read = lines.splitlines()
                if len(read) < 2:
                    raise SourceFileError(
                        'Invalid Source File, missing Header and/or Footer', file_name
                    )
                last_line = read[-1]
                data = pd.read_fwf(
                    StringIO(last_line),
                    colspecs=config.specs,
                    names=config.names,
                    converters={h: str for h in config.names},
                    delimiter='\n\t',
                )
                return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class BodyOnlySourceFormatter(AbstractDataMapper):
    def run(self, config: SourceFormatterConfig, file_name: str) -> pd.DataFrame:
        try:
            data = pd.read_fwf(
                file_name,
                colspecs=config.specs,
                header=None,
                names=config.names,
                converters={h: str for h in config.names},
                skipfooter=0,
                delimiter='\n\t',
            )
            if len(data.index) == 0:
                raise SourceFileError('Invalid Source File, Index is empty', file_name)
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)
