import os
import uuid

import numpy as np
import pytest

from transformer.source_mapper import HeaderDataMapper, BodyDataMapper, FooterDataMapper
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
            mapping = {
                "names": ["field1", "field2"],
                "specs": [(0, 10), (10, 20)]
            }

            values = ["val1", "val2"]
            spacing = [10, 10]
            with open(file_name, 'w') as file:
                file.write(generate_fw_text_line(["val1", "val2"], spacing))

            data = HeaderDataMapper(mapping).run(file_name)
            expected = ["val1      ", "val2      "]
            for itr in range(len(values)):
                assert data[mapping['names'][itr]][0] == expected[itr]

        def test_nan(self, file_name):
            mapping = {
                "names": ["field1", "field2"],
                "specs": [(0, 10), (50, 60)]
            }
            values = ["val1", "val2"]
            spacing = [10, 10]
            with open(file_name, 'w') as file:
                file.write(generate_fw_text_line(["val1", "val2"], spacing))

            data = HeaderDataMapper(mapping).run(file_name)
            count = data.isnull().values.sum()
            assert count == 1

    class TestFailure:
        def test_empty_file(self, file_name):
            mapping = {
                "names": ["field1", "field2"],
                "specs": [(0, 10), (10, 20)]
            }
            with open(file_name, 'w') as file:
                pass
            with pytest.raises(SourceFileError):
                HeaderDataMapper(mapping).run(file_name)

        def test_missing_file(self, file_name):
            mapping = {
                "names": ["field1", "field2"],
                "specs": [(0, 10), (10, 20)]
            }
            with pytest.raises(SourceFileError):
                HeaderDataMapper(mapping).run(file_name)


class TestBodyDataMapper:
    @pytest.fixture(autouse=True)
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    class TestSuccess:
        def test_body(self, file_name):
            mapping = {
                "names": ["field1", "field2", "field3"],
                "specs": [(0, 10), (10, 20), (20, 25)],
                "skipFooter": False,
                "skipHeader": False,
            }

            values = [["1", "1", "1"], ["2", "2", "2"], ["3", "3", "3"]]
            spacing = [10, 10, 5]
            with open(file_name, 'w') as file:
                for v in values:
                    file.write(generate_fw_text_line(v, spacing))
                    file.write("\n")

            data = BodyDataMapper(mapping).run(file_name)
            print(data)
            assert not data.isnull().values.any()
            assert len(data.index) == 3

        def test_nan(self, file_name):
            mapping = {
                "names": ["field1", "field2", "field3"],
                "specs": [(0, 10), (10, 20), (50, 51)],
                "skipFooter": False,
                "skipHeader": False,
            }

            values = [["1", "1", "1"], ["2", "2", "2"], ["3", "3", "3"]]
            spacing = [10, 10, 5]
            with open(file_name, 'w') as file:
                for v in values:
                    file.write(generate_fw_text_line(v, spacing))
                    file.write("\n")

            data = BodyDataMapper(mapping).run(file_name)
            print(data)
            count = data.isnull().values.sum()
            assert count == 3

    class TestFailure:
        def test_empty_file(self, file_name):
            mapping = {
                "names": ["field1", "field2"],
                "specs": [(0, 10), (10, 20)],
                "skipHeader": False,
                "skipFooter": False,
            }
            with open(file_name, 'w'):
                pass
            with pytest.raises(SourceFileError):
                BodyDataMapper(mapping).run(file_name)

        def test_missing_file(self, file_name):
            mapping = {
                "names": ["field1", "field2"],
                "specs": [(0, 10), (10, 20)],
                "skipHeader": False,
                "skipFooter": False,
            }
            with pytest.raises(SourceFileError):
                BodyDataMapper(mapping).run(file_name)


def generate_fw_text_line(values: list[str], spacing: [10,10]):
    if len(values) != len(spacing):
        raise Exception("values and spacing must be of the same length")
    text = ""
    for itr in range(len(values)):
        text += values[itr] + (" "* (spacing[itr] - len(values[itr])))

    return text
