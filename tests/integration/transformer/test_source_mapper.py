from tests.test_helper import generate_file_data
from transformer.executor import ExecutorConfig
from transformer.library.exceptions import ValidationFailureError
from transformer.source import SourceMapperConfig, source_formatter, SourceMapper
import pytest
import os
import uuid


class TestSourceMapper:
    @pytest.fixture(autouse=True)
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    class TestSuccess:
        # Arrange
        def test_with_validation(self, file_name):
            cfg = """
            files:
                Amazing File:
                    pattern: ^somefile.txt$
                    source:
                        header:
                            formatter: HeaderSourceFormatter
                            format:
                                - name: batchId
                                  spec: 0,50
                                  validators:
                                    - name: RegexValidator
                                      arguments:
                                        pattern: ^.{50}$
                        body:
                            formatter: BodySourceFormatter
                            format:
                                - name: Name
                                  spec: 0,50
                                  validators:
                                    - name: RegexValidator
                                      arguments:
                                        pattern: ^.+$
                        footer:
                            formatter: FooterSourceFormatter
                            format:
                                - name: field1
                                  spec: 0,5
            """

            file_data = {
                "header": {
                    "values": [[uuid.uuid4().__str__()]],
                    "spacing": [50]
                },
                "body": {
                    "values": [
                        ["ra"], ["roo"], ["raroola"], ["tee"]
                    ],
                    "spacing": [50]
                },
                "footer": {
                    "values": [["ABCD"]],
                    "spacing": [5]
                }
            }
            generate_file_data(file_name, file_data)
            # Act
            executor_cfg = ExecutorConfig(key="somefile.txt", inline=cfg)
            sm_config = SourceMapperConfig(executor_cfg.get_exact_config(), file_name)
            dfs = {}
            print(sm_config.get_mappers())
            for mapper_cfg in sm_config.get_mappers():
                dfs[mapper_cfg.segment] = getattr(source_formatter, mapper_cfg.name)().run(mapper_cfg, file_name)

            print(dfs)
            # Assert
            for df in dfs:
                assert len(dfs[df].index) == len(file_data[df]['values'])

            # Clean up

        def test_without_validation(self, file_name):
            # Arrange
            cfg = """
            files:
                Amazing File:
                    pattern: ^somefile.txt$
                    source:
                        header:
                            formatter: HeaderSourceFormatter
                            format:
                                - name: batchId
                                  spec: 0,50
                                  validators:
                                    - name: RegexValidator
                                      arguments:
                                        pattern: ^.{50}$
                        body:
                            formatter: BodySourceFormatter
                            format:
                                - name: Name
                                  spec: 0,50
                                  validators:
                                    - name: RegexValidator
                                      arguments:
                                        pattern: ^.+$
                        footer:
                            formatter: FooterSourceFormatter
                            format:
                                - name: field1
                                  spec: 0,5
            """
            file_data = {
                "header": {
                    "values": [[uuid.uuid4().__str__()]],
                    "spacing": [50]
                },
                "body": {
                    "values": [
                        ["ra"], ["roo"], ["raroola"], ["tee"]
                    ],
                    "spacing": [50]
                },
                "footer": {
                    "values": [["ABCD"]],
                    "spacing": [5]
                }
            }
            generate_file_data(file_name, file_data)

            # Act
            executor_cfg = ExecutorConfig(key="somefile.txt", inline=cfg)
            sm_config = SourceMapperConfig(executor_cfg.get_exact_config(), file_name)
            dfs = {}
            print(sm_config.get_mappers())
            for mapper_cfg in sm_config.get_mappers():
                dfs[mapper_cfg.segment] = getattr(source_formatter, mapper_cfg.name)().run(mapper_cfg, file_name)

            # Assert
            print(dfs)
            for df in dfs:
                assert len(dfs[df].index) == len(file_data[df]['values'])

    class TestFailure:
        def test_validation_issues(self, file_name):
            # Arrange
            cfg = """
            files:
                Amazing File:
                    pattern: ^somefile.txt$
                    source:
                        header:
                            formatter: HeaderSourceFormatter
                            format:
                                - name: batchId
                                  spec: 0,50
                        body:
                            formatter: BodySourceFormatter
                            format:
                                - name: Name
                                  spec: 0,50
                                  validators:
                                    - name: RegexValidator
                                      arguments:
                                        pattern: ^.{1}$
                        footer:
                            formatter: FooterSourceFormatter
                            format:
                                - name: field1
                                  spec: 0,5
            """
            file_data = {
                "header": {
                    "values": [[uuid.uuid4().__str__()]],
                    "spacing": [50]
                },
                "body": {
                    "values": [
                        ["ra"], ["roo"], ["raroola"], ["tee"]
                    ],
                    "spacing": [50]
                },
                "footer": {
                    "values": [["ABCD"]],
                    "spacing": [5]
                }
            }
            generate_file_data(file_name, file_data)

            # Act
            executor_cfg = ExecutorConfig(key="somefile.txt", inline=cfg)
            sm_config = SourceMapperConfig(executor_cfg.get_exact_config(), file_name)
            with pytest.raises(ValidationFailureError) as e:
                SourceMapper().run(sm_config)
