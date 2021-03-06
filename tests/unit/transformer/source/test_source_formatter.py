import os
import uuid
from tests.test_helper import generate_fw_text_line, generate_file_data
import pytest

from transformer.source.source_formatter import HeaderSourceFormatter, BodySourceFormatter, FooterSourceFormatter, SourceFormatterConfig, BodyOnlySourceFormatter
from transformer.library.exceptions import SourceFileError


class TestHeaderSourceFormatter:
    @pytest.fixture(autouse=True)
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    def test_header(self, file_name):
        values = ["val1", "val2"]
        spacing = [10, 10]
        with open(file_name, 'w') as file:
            file.write(generate_fw_text_line(values, spacing))
        config = SourceFormatterConfig(
            name="HeaderSourceFormatter",
            segment='header',
            names=["field1", "field2"],
            specs=[(0, 10), (10, 20)],
        )
        data = HeaderSourceFormatter().run(config, file_name)
        expected = ["val1      ", "val2      "]
        for itr in range(len(values)):
            assert data[config.names[itr]][0] == expected[itr]

    def test_nan(self, file_name):
        config = SourceFormatterConfig(
            name="HeaderSourceFormatter",
            segment='header',
            names=["field1", "field2"],
            specs=[(0, 10), (50, 60)]
        )
        values = ["val1", "val2"]
        spacing = [10, 10]
        with open(file_name, 'w') as file:
            file.write(generate_fw_text_line(values, spacing))

        data = HeaderSourceFormatter().run(config, file_name)
        count = data.isnull().values.sum()
        assert count == 1

    def test_empty_file(self, file_name):
        config = SourceFormatterConfig(
            name="HeaderSourceFormatter",
            segment='header',
            names=["field1", "field2"],
            specs=[(0, 10), (10, 20)],
        )
        with open(file_name, 'w') as file:
            pass
        with pytest.raises(SourceFileError):
            HeaderSourceFormatter().run(config, file_name)

    def test_missing_file(self, file_name):
        config = SourceFormatterConfig(
            name="HeaderSourceFormatter",
            segment='header',
            names=["field1", "field2"],
            specs=[(0, 10), (10, 20)],
        )
        with pytest.raises(SourceFileError):
            HeaderSourceFormatter().run(config, file_name)


class TestBodySourceFormatter:
    @pytest.fixture(autouse=True)
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    # def test_body(self, file_name):
    #     config = SourceFormatterConfig(
    #         name="BodySourceFormatter",
    #         validations=[],
    #         segment='body',
    #         names=["field1", "field2", "field3"],
    #         specs=[(0, 10), (10, 20), (20, 25)],
    #     )
    #     values = [["1", "1", "1"], ["2", "2", "2"], ["3", "3", "3"]]
    #     spacing = [10, 10, 5]
    #     with open(file_name, 'w') as file:
    #         for v in values:
    #             file.write(generate_fw_text_line(v, spacing))
    #             file.write("\n")
    # 
    #     data = BodySourceFormatter().run(config, file_name)
    #     print(data)
    #     assert not data.isnull().values.any()
    #     assert len(data.index) == 2
    # 
    # def test_nan(self, file_name):
    #     config = SourceFormatterConfig(
    #         name="BodySourceFormatter",
    #         validations=[],
    #         segment='body',
    #         names=["field1", "field2", "field3"],
    #         specs=[(0, 10), (10, 20), (50, 51)],
    #     )
    #     values = [["1", "1", "1"], ["2", "2", "2"], ["3", "3", "3"]]
    #     spacing = [10, 10, 5]
    #     with open(file_name, 'w') as file:
    #         for v in values:
    #             file.write(generate_fw_text_line(v, spacing))
    #             file.write("\n")
    # 
    #     data = BodySourceFormatter().run(config, file_name)
    #     print(data)
    #     count = data.isnull().values.sum()
    #     assert count == 2

    def test_empty_file(self, file_name):
        config = SourceFormatterConfig(
            name="BodySourceFormatter",
            segment='body',
            names=["field1", "field2", "field3"],
            specs=[(0, 10), (10, 20), (50, 51)],
        )
        with open(file_name, 'w'):
            pass
        with pytest.raises(SourceFileError):
            BodySourceFormatter().run(config, file_name)

    def test_missing_file(self, file_name):
        config = SourceFormatterConfig(
            name="BodySourceFormatter",
            segment='body',
            names=["field1", "field2", "field3"],
            specs=[(0, 10), (10, 20), (50, 51)],
        )
        with pytest.raises(SourceFileError):
            BodySourceFormatter().run(config, file_name)


class TestFooterSourceFormatter:
    @pytest.fixture(autouse=True)
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    def test_header(self, file_name):
        config = SourceFormatterConfig(
            name="FooterSourceFormatter",
            segment='footer',
            names=["field1", "field2"],
            specs=[(0, 10), (10, 20)],
        )
        values = ["val1", "val2"]
        spacing = [10, 10]
        with open(file_name, 'w') as file:
            file.write(generate_fw_text_line(values, spacing))

        data = FooterSourceFormatter().run(config, file_name)
        expected = ["val1      ", "val2      "]
        for itr in range(len(values)):
            assert data[config.names[itr]][0] == expected[itr]

    def test_nan(self, file_name):
        config = SourceFormatterConfig(
            name="FooterSourceFormatter",
            segment='footer',
            names=["field1", "field2"],
            specs=[(0, 10), (50, 60)],
        )
        values = ["val1", "val2"]
        spacing = [10, 10]
        with open(file_name, 'w') as file:
            file.write(generate_fw_text_line(values, spacing))

        data = FooterSourceFormatter().run(config, file_name)
        count = data.isnull().values.sum()
        assert count == 1

    def test_empty_file(self, file_name):
        config = SourceFormatterConfig(
            name="FooterSourceFormatter",
            segment='footer',
            names=["field1", "field2"],
            specs=[(0, 10), (50, 60)],
        )
        with open(file_name, 'w') as file:
            pass
        with pytest.raises(SourceFileError):
            FooterSourceFormatter().run(config, file_name)

    def test_missing_file(self, file_name):
        config = SourceFormatterConfig(
            name="FooterSourceFormatter",
            segment='footer',
            names=["field1", "field2"],
            specs=[(0, 10), (50, 60)],
        )
        with pytest.raises(SourceFileError):
            FooterSourceFormatter().run(config, file_name)


class TestBodyOnlySourceFormatter:
    @pytest.fixture(autouse=True)
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    def test_success(self, file_name):
        config = SourceFormatterConfig(
            name="BodyOnlySourceFormatter",
            segment="body",
            names=["field1", "field2"],
            specs=[(0, 10), (10, 20)],
        )
        file_data = {
            "body": {
                "values": [["X1", "X2"], ["Y1", "Y2"], ["Z1", "Z2"]],
                "spacing": [10, 10]
            }
        }
        generate_file_data(file_name, file_data)
        df = BodyOnlySourceFormatter().run(config, file_name)
        assert len(df.index) == len(file_data['body']['values'])
    
    def test_empty_file(self, file_name):
        config = SourceFormatterConfig(
            name="BodyOnlySourceFormatter",
            segment='body',
            names=["field1", "field2"],
            specs=[(0, 10), (10, 20)],
        )
        with open(file_name, 'w'):
            pass
        with pytest.raises(SourceFileError):
            BodyOnlySourceFormatter().run(config, file_name)

    def test_missing_file(self, file_name):
        config = SourceFormatterConfig(
            name="BodySourceFormatter",
            segment='body',
            names=["field1", "field2"],
            specs=[(0, 10), (10, 20)],
        )
        with pytest.raises(SourceFileError):
            BodyOnlySourceFormatter().run(config, file_name)