import pandas as pd

from transformer.executor import executor, executor_config
from transformer.result import result_mapper
import pytest
import os
import uuid


class TestResultMapper:
    @pytest.fixture
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    def test_no_validations(self, file_name):
        text = f"""
        files:
            My First File:
                pattern: ^{file_name}$
                source:

                output:
                    mapper: DefaultArrayResultFormatter
                    format:
                        metadata:
                            - name: referenceId
                              value: header.referenceId
                            - name: batchId
                              value: UuidGenerator
                        body:
                            - name: AmazingField
                              value: body.field1
                            - name: NotAnAmazingField
                              value: body.field2
        """

        cfg = executor_config.ExecutorConfig(key=file_name, inline=text)
        result_cfg = result_mapper.ResultMapperConfig(config=cfg.get_exact_config())
        mapper = result_mapper.ResultMapper(result_cfg)
        dataframes = {
            "header": pd.DataFrame({
                "referenceId": ["AREYOUSUREYOUNEEDTHIS"]
            }),

            "body": pd.DataFrame(
                {
                    "field1": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    "field2": ["One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine", "Ten"],
                    "rubbishField": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
                }
            )
        }
        results = mapper.run(dataframes)
        assert isinstance(results, list)
        assert len(results) == 10