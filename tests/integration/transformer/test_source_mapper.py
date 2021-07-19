from tests.test_helper import generate_fw_text_line
from transformer import config
from transformer.library import exceptions
from transformer.source import source_mapper
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
                            mapper: HeaderDataMapper
                            format:
                                - name: batchId
                                  spec: 0,50
                                  validators:
                                    - name: RegexValidator
                                      arguments:
                                        pattern: ^.{50}$
                        body:
                            mapper: BodyDataMapper
                            format:
                                - name: Name
                                  spec: 0,50
                                  validators:
                                    - name: RegexValidator
                                      arguments:
                                        pattern: ^.+$
            """
            header_values = [uuid.uuid4().__str__()]
            body_values = [["ra"], ["roo"], ["raroola"], ["tee"]]
            with open(file_name, 'w') as file:
                file.write(generate_fw_text_line(header_values, [50]))
                file.write("\n")
                for v in body_values:
                    file.write(generate_fw_text_line(v, [50]))
                    file.write("\n")

            # Act
            executor_cfg = config.ExecutorConfig(key="somefile.txt",inline=cfg)
            sm_config = config.SourceMapperConfig(executor_cfg)
            dfs = {}
            print(sm_config.get_mappers())
            for mapper_cfg in sm_config.get_mappers():
                dfs[mapper_cfg.segment] = getattr(source_mapper, mapper_cfg.name)().run(mapper_cfg, file_name)

            print(dfs)
            # Assert
            print(dfs['header'])
            assert len(dfs['header'].index) == len(header_values)
            assert len(dfs['body'].index) == len(body_values)

            # Clean up

        def test_without_validation(self, file_name):
            # Arrange
            cfg = """
            files:
                Amazing File:
                    pattern: ^somefile.txt$
                    source:
                        header:
                            mapper: HeaderDataMapper
                            format:
                                - name: batchId
                                  spec: 0,50
                                  validators:
                                    - name: RegexValidator
                                      arguments:
                                        pattern: ^.{50}$
                        body:
                            mapper: BodyDataMapper
                            format:
                                - name: Name
                                  spec: 0,50
                                  validators:
                                    - name: RegexValidator
                                      arguments:
                                        pattern: ^.+$
            """
            header_values = [uuid.uuid4().__str__()]
            body_values = [["ra"], ["roo"], ["raroola"], ["tee"]]
            with open(file_name, 'w') as file:
                file.write(generate_fw_text_line(header_values, [50]))
                file.write("\n")
                for v in body_values:
                    file.write(generate_fw_text_line(v, [50]))
                    file.write("\n")

            # Act
            executor_cfg = config.ExecutorConfig(key="somefile.txt", inline=cfg)
            sm_config = config.SourceMapperConfig(executor_cfg)
            dfs = {}
            print(sm_config.get_mappers())
            for mapper_cfg in sm_config.get_mappers():
                dfs[mapper_cfg.segment] = getattr(source_mapper, mapper_cfg.name)().run(mapper_cfg, file_name)

            # Assert
            print(dfs)
            assert len(dfs['header'].index) == len(header_values)
            assert len(dfs['body'].index) == len(body_values)

    class TestFailure:
        def test_validation_issues(self, file_name):
            # Arrange
            cfg = """
                        files:
                            Amazing File:
                                pattern: ^somefile.txt$
                                source:
                                    header:
                                        mapper: HeaderDataMapper
                                        format:
                                            - name: batchId
                                              spec: 0,50
                                    body:
                                        mapper: BodyDataMapper
                                        format:
                                            - name: Name
                                              spec: 0,50
                                              validators:
                                                - name: RegexValidator
                                                  arguments:
                                                    pattern: ^.{1}$
                        """
            header_values = [uuid.uuid4().__str__()]
            body_values = [["ra"], ["roo"], ["raroola"], ["tee"]]
            with open(file_name, 'w') as file:
                file.write(generate_fw_text_line(header_values, [50]))
                file.write("\n")
                for v in body_values:
                    file.write(generate_fw_text_line(v, [50]))
                    file.write("\n")

            # Act
            executor_cfg = config.ExecutorConfig(key="somefile.txt", inline=cfg)
            sm_config = config.SourceMapperConfig(executor_cfg)
            dfs = {}
            with pytest.raises(exceptions.ValidationError):
                for mapper_cfg in sm_config.get_mappers():
                    dfs[mapper_cfg.segment] = getattr(source_mapper, mapper_cfg.name)().run(mapper_cfg, file_name)

