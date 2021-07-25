import pytest

from transformer.result import ResultMapperConfig


class TestResultMapperConfig:
    @pytest.fixture
    def pre_config(self):
        return {
            "result": {
                "formatter": "DefaultJsonArrayFormatter",
                "format": {}
            }
        }

    def test_result_mapper_no_validation(self, pre_config):
        pre_config['result']['format'] = {
                    "header": [
                        {
                            "name": "field1",
                            "value": "header.field1"
                        }
                    ]
                }
        config = ResultMapperConfig(pre_config)
        assert config.format
        assert len(config.format.formats) == 1
        assert len(config.validators) == 0

    def test_result_mapper_with_validation(self, pre_config):
        pre_config['result']['format'] = {
                    "header": [
                        {
                            "name": "field1",
                            "value": "header.field1",
                            "validators": [
                                {
                                    "name": "RegexValidator",
                                    "arguments": {
                                        "pattern": "^test$"
                                    }
                                },
                                {
                                    "name": "NricValidator"
                                }
                            ]
                        }
                    ]
                }
        config = ResultMapperConfig(pre_config)
        assert config.format
        assert len(config.format.formats) == 1
        assert len(config.validators) == 2

    def test_result_mapper_no_format(self, pre_config):
        config = ResultMapperConfig(pre_config)
        assert not config.format.formats
        assert len(config.validators) == 0

    # def test_result_mapper_root_depth(self, pre_config):
    #     pre_config['result']['format'] = [
    #         {
    #             "name": "field1",
    #             "value": "header.field1"
    #         },
    #         {
    #             "name": "field2",
    #             "value": "UuidGenerator"
    #         }
    #     ]
    #     config = ResultMapperConfig(pre_config)
    #     assert config.format
    #     assert len(config.validators) == 0