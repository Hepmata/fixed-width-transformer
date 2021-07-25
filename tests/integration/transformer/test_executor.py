import unittest.mock

import yaml

from transformer.executor import LambdaFixedWidthExecutor
from tests.test_helper import generate_file_data
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
                trim: True
                nan_check: True
                source:
                    header:
                        formatter: HeaderSourceFormatter
                        format:
                            - name: header1
                              spec: 0,1
                            - name: header2
                              spec: 1,5
                            - name: header3
                              spec: 5,20
                    body:
                        formatter: BodySourceFormatter
                        format:
                            - name: body1
                              spec: 0,1
                            - name: body2
                              spec: 1,5
                    footer:
                        formatter: FooterSourceFormatter
                        format:
                            - name: footer1
                              spec: 0, 5
                result:
                    producer:
                        name: ConsoleResult
                    formatter: DefaultArrayResultFormatter
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

        file_data = {
            "header": {
                "values": [["X", "1234", "G4114"]],
                "spacing": [1, 4, 15]
            },
            "body": {
                "values": [["B", "1"], ["B", "2"], ["B", "3"], ["B", "4"]],
                "spacing": [1, 4]
            },
            "footer": {
                "values": [["FT"]],
                "spacing": [5]
            }
        }
        generate_file_data("/tmp/"+file_name, file_data)

        with(open(cfg_file, 'w')) as file:
            yaml.dump(yaml.load(text), file)
        results = LambdaFixedWidthExecutor().run(key=file_name, bucket="")
