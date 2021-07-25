from transformer.source import SourceMapperConfig, SourceMapper, SourceFormatterConfig
from tests.test_helper import generate_fw_text_line, generate_file_data
import pytest
import os
import uuid


class TestSourceMapper:
    @pytest.fixture
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    def test_1_segment_without_validation(self):
        pass

    def test_1_segment_with_validation(self):
        pass

    def test_2_segment_without_validation(self):
        pass

    def test_2_segment_with_validation(self):
        pass

    def test_3_segment_without_validation(self):
        pass

    def test_3_segment_with_validator_and_converter(self, file_name):
        config_dict = {
            "pattern": "somepattern",
            "trim": True,
            "checkNan": True,
            "source": {
                "header": {
                    "formatter": "HeaderSourceFormatter",
                    "format": [
                        {
                            "name": "field1",
                            "spec": "0, 4"
                        }
                    ]
                },
                "body": {
                    "formatter": "BodySourceFormatter",
                    "format": [
                        {
                            "name": "two",
                            "spec": "0, 10",
                            "converter": "NumberConverter",
                            "validators": [
                                {
                                    "name": "RegexValidator",
                                    "arguments": {
                                        "pattern": "^.{10}$"
                                    }
                                }
                            ]
                        }
                    ]
                },
                "footer": {
                    "formatter": "FooterSourceFormatter",
                    "format": [
                        {
                            "name": "footer1",
                            "spec": "0,5"
                        }
                    ]
                }
            }
        }
        file_data = {
            "header": {
                "values": [["12"]],
                "spacing": [4]
            },
            "body": {
                "values": [
                    ["10"], ["11"], ["12"]
                ],
                "spacing": [10]
            },
            "footer": {
                "values": [['1X']],
                "spacing": [5]
            }
        }
        generate_file_data(file_name, file_data)

        config = SourceMapperConfig(config_dict, file_name)
        dataframes = SourceMapper().run(config)
        print(dataframes)
        assert len(dataframes.keys()) == 3
        for df in dataframes:
            assert len(dataframes[df].index) == len(file_data[df]['values'])
        assert isinstance(dataframes['body']['two'][0].item(), int)
