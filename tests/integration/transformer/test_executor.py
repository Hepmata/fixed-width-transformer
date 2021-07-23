import unittest.mock

import yaml

from transformer.executor import ExecutorConfig, LambdaFixedWidthExecutor
from tests.test_helper import generate_fw_text_line
import pytest
import os
import uuid


class TestLambdaFixedWidthExecutor:
    @pytest.fixture
    def file_name(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    @pytest.fixture
    def cfg_file(self):
        source_file_name = f"fw_file-{uuid.uuid4().__str__()}.txt"
        yield source_file_name
        if os.path.exists(source_file_name):
            os.remove(source_file_name)

    def test_with_result_format(self, file_name, cfg_file, mocker):
        unittest.mock.patch.dict('os.environ', {'config_type': 'local', 'config_name': cfg_file}).start()
        mocker.patch('transformer.library.aws_service.download_s3_file').return_value = file_name
        text = f"""
        files:
            File 1:
                pattern: ^{file_name}$
                source:
                    header:
                        mapper: HeaderDataMapper
                        format:
                            - name: header1
                              spec: 0,1
                            - name: header2
                              spec: 1,5
                            - name: header3
                              spec: 5,20
                    body:
                        mapper: BodyDataMapper
                        format:
                            - name: body1
                              spec: 0,1
                            - name: body2
                              spec: 1,5
                    footer:
                        mapper: FooterDataMapper
                        format:
                            - name: footer1
                              spec: 0, 5
                output:
                    result:
                        name: ConsoleResult
                        arguments:
                            arg1: test
                    mapper: DefaultArrayResultFormatter
                    format:
                        metadata:
                            - name: field1
                              value: header.header1
                        body:
                            - name: body1
                              value: body.body1
                            - name: body2
                              value: body.body2
                            - name: record_id
                              value: UuidGenerator
        """

        header_values = ["X", "1234", "G4114"]
        header_spacing = [1, 4, 15]
        body_values = [
            ["B", "1"],
            ["B", "2"],
            ["B", "3"],
            ["B", "4"]
        ]
        body_spacing = [1, 4]
        footer_values = ["FT"]
        footer_spacing = [5]

        with open(file_name, 'w') as file:
            file.write(generate_fw_text_line(header_values, header_spacing))
            file.write("\n")
            for val in body_values:
                file.write(generate_fw_text_line(val, body_spacing))
                file.write("\n")
            file.write(generate_fw_text_line(footer_values, footer_spacing))
            file.write("\n")

        with(open(cfg_file, 'w')) as file:
            yaml.dump(yaml.load(text), file)
        results = LambdaFixedWidthExecutor().run(key=file_name, bucket="")
