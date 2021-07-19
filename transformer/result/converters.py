import pandas as pd


class AbstractConverter:
    data: pd.Series

    def __init__(self, data: pd.Series):
        self.data = data

    def convert(self): pass


class StrConverter(AbstractConverter):

    def __init__(self, data: pd.Series):
        super().__init__(data)

    def convert(self):
        return self.data.str.strip()


class IntConverter(AbstractConverter):
    def __init__(self, data: pd.Series):
        super().__init__(data)

    def convert(self):
        return pd.to_numeric(self.data)