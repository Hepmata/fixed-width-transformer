import pandas as pd
import pytest

from transformer.converter import ConverterConfig, converter
from transformer.library.exceptions import ConversionError


class TestStrConverter:
    class TestSuccess:
        def test_int_to_str(self):
            config = ConverterConfig(
                segment="body",
                field_name="test1",
                name="StrConverter"
            )
            dataframes = {
                "header": None,
                "body": pd.DataFrame({
                    "test1": [1, 2, 3, 4, 5]
                })
            }
            results = converter.StrConverter(config).run(dataframes)
            print(results)
            assert len(results.index) == len(dataframes[config.segment][config.field_name].index)
            for f in results:
                assert isinstance(f, str)

        def test_float_to_int(self):
            config = ConverterConfig(
                segment="body",
                field_name="test1",
                name="StrConverter"
            )
            dataframes = {
                "header": None,
                "body": pd.DataFrame({
                    "test1": [1.0, 2.0, 3.0, 4.0, 5.0]
                })
            }
            results = converter.StrConverter(config).run(dataframes)
            print(results)
            assert len(results.index) == len(dataframes[config.segment][config.field_name].index)
            for f in results:
                assert isinstance(f, str)

        def test_str_to_str(self):
            config = ConverterConfig(
                segment="body",
                field_name="test1",
                name="StrConverter"
            )
            dataframes = {
                "header": None,
                "body": pd.DataFrame({
                    "test1": ["lalala", "lololol", "lelelele"]
                })
            }
            results = converter.StrConverter(config).run(dataframes)
            print(results)

            assert len(results.index) == len(dataframes[config.segment][config.field_name].index)
            for f in results:
                assert isinstance(f, str)

        def test_escaped_str_to_str(self):
            config = ConverterConfig(
                segment="body",
                field_name="test1",
                name="StrConverter"
            )
            dataframes = {
                "header": None,
                "body": pd.DataFrame({
                    "test1": ["\"asda\"\"asdasd", "\"asd\"\"asdlololol", "lelelele"]
                })
            }
            results = converter.StrConverter(config).run(dataframes)
            print(results)

            assert len(results.index) == len(dataframes[config.segment][config.field_name].index)
            for f in results:
                assert isinstance(f, str)

class TestNumberConverter:
    class TestSuccess:
        def test_str_to_int(self):
            config = ConverterConfig(
                segment="body",
                field_name="field1",
                name="IntConverter"
            )
            dataframes = {
                "body": pd.DataFrame({
                    "field1": ["1", "2", "3", "4", "5"]
                })
            }
            results = converter.NumberConverter(config).run(dataframes)
            print(results)
            assert len(results.index) == len(dataframes[config.segment][config.field_name].index)
            for f in results:
                assert isinstance(f, int)
        
        def test_str_to_float(self):
            config = ConverterConfig(
                segment="body",
                field_name="field1",
                name="IntConverter"
            )
            dataframes = {
                "body": pd.DataFrame({
                    "field1": ["1.0", "2.0", "3.0", "4.0", "5.0"]
                })
            }
            results = converter.NumberConverter(config).run(dataframes)
            print(results)
            assert len(results.index) == len(dataframes[config.segment][config.field_name].index)
            for f in results:
                assert isinstance(f, float)

        def test_invalid_to_int(self):
            config = ConverterConfig(
                segment="body",
                field_name="field1",
                name="IntConverter"
            )
            dataframes = {
                "body": pd.DataFrame({
                    "field1": ["adasd", "*$)_@#(_)@#", "1"]
                })
            }
            with pytest.raises(ConversionError):
                converter.NumberConverter(config).run(dataframes)
        
        def test_invalid_to_float(self):
            config = ConverterConfig(
                segment="body",
                field_name="field1",
                name="IntConverter"
            )
            dataframes = {
                "body": pd.DataFrame({
                    "field1": ["adasd", "*$)_@#(_)@#", "1"]
                })
            }
            with pytest.raises(ConversionError):
                converter.NumberConverter(config).run(dataframes)