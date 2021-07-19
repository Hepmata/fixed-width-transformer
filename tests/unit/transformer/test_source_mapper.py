import os
import uuid
from tests.test_helper import generate_fw_text_line
import pytest

from transformer.source_mapper import HeaderDataMapper, BodyDataMapper, FooterDataMapper, MapperConfig
from transformer.library.exceptions import SourceFileError


class TestHeaderDataMapper:
    @pytest.fixture(autouse=True)
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    class TestSuccess:
        def test_header(self, file_name):

            values = ["val1", "val2"]
            spacing = [10, 10]
            with open(file_name, 'w') as file:
                file.write(generate_fw_text_line(values, spacing))
            config = MapperConfig(
                name="HeaderDataMapper",
                validations=[],
                segment='header',
                names=["field1", "field2"],
                specs=[(0, 10), (10, 20)],
                skipHeader=False,
                skipFooter=True
            )
            data = HeaderDataMapper().run(config, file_name)
            expected = ["val1      ", "val2      "]
            for itr in range(len(values)):
                assert data[config.names[itr]][0] == expected[itr]

        def test_nan(self, file_name):
            config = MapperConfig(
                name="HeaderDataMapper",
                validations=[],
                segment='header',
                names=["field1", "field2"],
                specs=[(0, 10), (50, 60)],
                skipHeader=False,
                skipFooter=True
            )
            values = ["val1", "val2"]
            spacing = [10, 10]
            with open(file_name, 'w') as file:
                file.write(generate_fw_text_line(values, spacing))

            data = HeaderDataMapper().run(config, file_name)
            count = data.isnull().values.sum()
            assert count == 1

    class TestFailure:
        def test_empty_file(self, file_name):
            config = MapperConfig(
                name="HeaderDataMapper",
                validations=[],
                segment='header',
                names=["field1", "field2"],
                specs=[(0, 10), (10, 20)],
                skipHeader=False,
                skipFooter=True
            )
            with open(file_name, 'w') as file:
                pass
            with pytest.raises(SourceFileError):
                HeaderDataMapper().run(config, file_name)

        def test_missing_file(self, file_name):
            config = MapperConfig(
                name="HeaderDataMapper",
                validations=[],
                segment='header',
                names=["field1", "field2"],
                specs=[(0, 10), (10, 20)],
                skipHeader=False,
                skipFooter=True
            )
            with pytest.raises(SourceFileError):
                HeaderDataMapper().run(config, file_name)


class TestBodyDataMapper:
    @pytest.fixture(autouse=True)
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    class TestSuccess:
        def test_body(self, file_name):
            config = MapperConfig(
                name="BodyDataMapper",
                validations=[],
                segment='body',
                names=["field1", "field2", "field3"],
                specs=[(0, 10), (10, 20), (20, 25)],
                skipHeader=False,
                skipFooter=False
            )
            values = [["1", "1", "1"], ["2", "2", "2"], ["3", "3", "3"]]
            spacing = [10, 10, 5]
            with open(file_name, 'w') as file:
                for v in values:
                    file.write(generate_fw_text_line(v, spacing))
                    file.write("\n")

            data = BodyDataMapper().run(config, file_name)
            print(data)
            assert not data.isnull().values.any()
            assert len(data.index) == 3

        def test_nan(self, file_name):
            config = MapperConfig(
                name="BodyDataMapper",
                validations=[],
                segment='body',
                names=["field1", "field2", "field3"],
                specs=[(0, 10), (10, 20), (50, 51)],
                skipHeader=False,
                skipFooter=False
            )
            values = [["1", "1", "1"], ["2", "2", "2"], ["3", "3", "3"]]
            spacing = [10, 10, 5]
            with open(file_name, 'w') as file:
                for v in values:
                    file.write(generate_fw_text_line(v, spacing))
                    file.write("\n")

            data = BodyDataMapper().run(config, file_name)
            print(data)
            count = data.isnull().values.sum()
            assert count == 3

    class TestFailure:
        def test_empty_file(self, file_name):
            config = MapperConfig(
                name="BodyDataMapper",
                validations=[],
                segment='body',
                names=["field1", "field2", "field3"],
                specs=[(0, 10), (10, 20), (50, 51)],
                skipHeader=False,
                skipFooter=False
            )
            with open(file_name, 'w'):
                pass
            with pytest.raises(SourceFileError):
                BodyDataMapper().run(config, file_name)

        def test_missing_file(self, file_name):
            config = MapperConfig(
                name="BodyDataMapper",
                validations=[],
                segment='body',
                names=["field1", "field2", "field3"],
                specs=[(0, 10), (10, 20), (50, 51)],
                skipHeader=False,
                skipFooter=False
            )
            with pytest.raises(SourceFileError):
                BodyDataMapper().run(config, file_name)


class TestFooterDataMapper:
    @pytest.fixture(autouse=True)
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    class TestSuccess:
        def test_header(self, file_name):
            config = MapperConfig(
                name="FooterDataMapper",
                validations=[],
                segment='footer',
                names=["field1", "field2"],
                specs=[(0, 10), (10, 20)],
                skipHeader=False,
                skipFooter=False
            )
            values = ["val1", "val2"]
            spacing = [10, 10]
            with open(file_name, 'w') as file:
                file.write(generate_fw_text_line(values, spacing))

            data = FooterDataMapper().run(config, file_name)
            expected = ["val1      ", "val2      "]
            for itr in range(len(values)):
                assert data[config.names[itr]][0] == expected[itr]

        def test_nan(self, file_name):
            config = MapperConfig(
                name="FooterDataMapper",
                validations=[],
                segment='footer',
                names=["field1", "field2"],
                specs=[(0, 10), (50, 60)],
                skipHeader=False,
                skipFooter=False
            )
            values = ["val1", "val2"]
            spacing = [10, 10]
            with open(file_name, 'w') as file:
                file.write(generate_fw_text_line(values, spacing))

            data = FooterDataMapper().run(config, file_name)
            count = data.isnull().values.sum()
            assert count == 1

    class TestFailure:
        def test_empty_file(self, file_name):
            config = MapperConfig(
                name="FooterDataMapper",
                validations=[],
                segment='footer',
                names=["field1", "field2"],
                specs=[(0, 10), (50, 60)],
                skipHeader=False,
                skipFooter=False
            )
            with open(file_name, 'w') as file:
                pass
            with pytest.raises(SourceFileError):
                FooterDataMapper().run(config, file_name)

        def test_missing_file(self, file_name):
            config = MapperConfig(
                name="FooterDataMapper",
                validations=[],
                segment='footer',
                names=["field1", "field2"],
                specs=[(0, 10), (50, 60)],
                skipHeader=False,
                skipFooter=False
            )
            with pytest.raises(SourceFileError):
                FooterDataMapper().run(config, file_name)
