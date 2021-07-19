from transformer.library import logger
from transformer.library.exceptions import SourceFileError
from io import StringIO
import pandas as pd

log = logger.set_logger(__name__)


class AbstractDataMapper:
    segment: str
    mapping = dict

    def __init__(self, segment: str, mapping: dict):
        self.segment = segment
        self.mapping = mapping

    def run(self, file_name): pass

    def get_segment(self):
        return self.segment


class HeaderDataMapper(AbstractDataMapper):
    def __init__(self, mapping: dict):
        super().__init__('header', mapping)

    def run(self, file_name) -> pd.DataFrame:
        try:
            data = pd.read_fwf(file_name, colspecs=self.mapping['specs'], names=self.mapping['names'],
                               converters={h: str for h in self.mapping['names']}, delimiter="\n\t", nrows=1)
            if len(data.index) == 0:
                raise SourceFileError("Invalid Source File, Index is empty", file_name)
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class SubHeaderDataMapper(AbstractDataMapper):
    def __init__(self, mapping: dict):
        super().__init__('subheader', mapping)

    def run(self, file_name):
        raise NotImplemented()


class BodyDataMapper(AbstractDataMapper):
    def __init__(self, mapping: dict):
        super().__init__('body', mapping)

    def run(self, file_name) -> pd.DataFrame:
        header = 0
        footer = 1
        if 'skipHeader' in self.mapping.keys():
            header = 0 if self.mapping['skipHeader'] else None
        if 'skipFooter' in self.mapping.keys():
            footer = 1 if self.mapping['skipFooter'] else 0
        try:
            data = pd.read_fwf(file_name, colspecs=self.mapping['specs'], header=header,
                               names=self.mapping['names'],
                               converters={h: str for h in self.mapping['names']}, skipfooter=footer,
                               delimiter="\n\t")
            if len(data.index) == 0:
                raise SourceFileError("Invalid Source File, Index is empty", file_name)
            return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)


class FooterDataMapper(AbstractDataMapper):
    def __init__(self, mapping: dict):
        super().__init__('footer', mapping)

    def run(self, file_name) -> pd.DataFrame:
        try:
            with open(file_name, 'r') as lines:
                read = lines.readlines()
                if len(read) == 0:
                    raise SourceFileError("Invalid Source File, Index is empty", file_name)
                last_line = read[-1]
                data = pd.read_fwf(StringIO(last_line), colspecs=self.mapping['specs'],
                                   names=self.mapping['names'],
                                   converters={h: str for h in self.mapping['names']}, delimiter="\n\t")
                return data
        except FileNotFoundError as e:
            raise SourceFileError(e, file_name)