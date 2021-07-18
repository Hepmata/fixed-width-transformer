from transformer.library import logger, exceptions
from io import StringIO
import pandas as pd

log = logger.set_logger(__name__)


class AbstractDataMapper:
    segment: str
    mapping = dict

    def __init__(self, segment: str, mapping: dict):
        self.segment = segment
        self.mapping = mapping

    def validate(self):
        raise exceptions.ValidationError("Failed to validate due to empty constraints")

    def run(self, file_name): pass

    def get_segment(self):
        return self.segment


class HeaderDataMapper(AbstractDataMapper):
    def __init__(self, mapping: dict):
        super().__init__('header', mapping)

    def run(self, file_name) -> pd.DataFrame:
        return pd.read_fwf(file_name, colspecs=self.mapping['specs'], names=self.mapping['names'],
                           converters={h: str for h in self.mapping['names']}, delimiter="\n\t", nrows=1)


class SubHeaderDataMapper(AbstractDataMapper):
    def __init__(self, mapping: dict):
        super().__init__('subheader', mapping)

    def run(self, file_name):
        raise NotImplemented()


class BodyDataMapper(AbstractDataMapper):
    def __init__(self, mapping: dict):
        super().__init__('body', mapping)

    def run(self, file_name) -> pd.DataFrame:
        return pd.read_fwf(file_name, colspecs=self.mapping['specs'], header=0,
                           names=self.mapping['names'],
                           converters={h: str for h in self.mapping['names']}, skipfooter=1,
                           delimiter="\n\t")


class FooterDataMapper(AbstractDataMapper):
    def __init__(self, mapping: dict):
        super().__init__('footer', mapping)

    def run(self, file_name) -> pd.DataFrame:
        with open(file_name, 'r') as lines:
            last_line = lines.readlines()[-1]
        return pd.read_fwf(StringIO(last_line), colspecs=self.mapping['specs'],
                           names=self.mapping['names'],
                           converters={h: str for h in self.mapping['names']}, delimiter="\n\t")
