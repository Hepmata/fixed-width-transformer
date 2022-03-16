import datetime
import os
import uuid
from os import listdir
from os.path import join, isfile
import pytest

from tests.helper import file_helper, aws_helper, kafka_helper
from tests.helper.fixtures import test_id
from transformer import handler
from unittest.mock import patch

project_root_path = os.path.abspath(os.path.dirname(__file__))
bucket = 'integration-test'
reference_bucket = 'reference-bucket'
kafka_broker = 'localhost:29091'


def teardown_module():
    excluded_files = ['docker-compose.yml', 'requirements.test.txt', 'requirements.txt', 'serverless.prd.yml',
                      'serverless.qa.yml', 'serverless.uat.yml', 'serverless.yml']
    files = [f for f in listdir('./') if isfile(join('./', f))]
    for file in files:
        if file.endswith('.txt') or file.endswith('.yml'):
            if file not in excluded_files:
                os.remove(file)


def setup_module():
    aws_helper.create_secret('test/kafka', {'username': 'client', 'password': 'password'}, uuid.uuid4().__str__())
    aws_helper.create_secret('test/sql', {'username': 'integration-test', 'password': 'integration-test'},
                             uuid.uuid4().__str__())
    aws_helper.create_s3_bucket(bucket)
    aws_helper.create_s3_bucket(reference_bucket)
    kafka_helper.create_topic(bootstrap_servers=kafka_broker,
                              topic_names=['disbursement-filetransformer-validated-result-giro-created'],
                              security_protocol='SASL_PLAINTEXT',
                              sasl_mechanism='SCRAM-SHA-512',
                              username='client',
                              password='password')


#
# @patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
# class TestHandler:
#     bootstrap_servers = 'localhost:29091'
#
#     def test_executor_with_constraints(self, test_id, mocker):
#         config_file_name = f'{test_id}.yml'
#         data_file_name = f'{test_id}.txt'
#         topic_name = f'{test_id}-topic'
#         config_content = f"""
#         files:
#             IntTestFile:
#                 pattern: ^{data_file_name}$
#                 trim: True
#                 nan_check: True
#                 source:
#                     header:
#                         formatter: HeaderSourceFormatter
#                         format:
#                             - name: header1
#                               spec: 0,10
#                     body:
#                         formatter: BodySourceFormatter
#                         format:
#                             - name: body1
#                               spec: 0,10
#                     footer:
#                         formatter: FooterSourceFormatter
#                         format:
#                             - name: footer1
#                               spec: 0,10
#                 result:
#                     formatter: DefaultArrayResultFormatter
#                     format:
#                         root:
#                             - name: header
#                               value: header.header1
#                             - name: body
#                               value: body.body1
#                             - name: footer
#                               value: footer.footer1
#                     producer:
#                         name: MSKScramResultProducer
#                         arguments:
#                             broker_urls: {self.bootstrap_servers}
#                             secret_name: test/kafka
#                             topic: {topic_name}
#                             security_protocol: SASL_PLAINTEXT
#                             sasl_mechanism: SCRAM-SHA-512
#         """
#         file_helper.create_config_file(config_content, config_file_name)
#         file_helper.create_fixed_width_file(
#             {
#                 'header': [{
#                     'length': 10,
#                     'value': 'tval1'
#                 }],
#                 'body': [{
#                     'length': 10,
#                     'value': 'bval1'
#                 }, {
#                     'length': 10,
#                     'value': 'bval2'
#                 }],
#                 'footer': [{
#                     'length': 10,
#                     'value': 'fval1'
#                 }]
#             }, data_file_name)
#         aws_helper.upload_s3_file(bucket='integration-bucket', file_name=data_file_name,
#                                   s3_file_key=data_file_name)
#         file_helper.delete_file(data_file_name)
#         context = aws_helper.MockContext(aws_request_id='123')
#         event = aws_helper.create_event('integration-bucket', data_file_name)
#         aws_helper.create_secret('test/kafka', {'username': 'client', 'password': 'password'}, test_id)
#         mocker.patch.dict(os.environ, {'config_name': config_file_name, 'mount_path': project_root_path})
#         kafka_helper.create_topic(bootstrap_servers=self.bootstrap_servers, topic_name=topic_name,
#                                   security_protocol='SASL_PLAINTEXT',
#                                   sasl_mechanism='SCRAM-SHA-512',
#                                   username='client',
#                                   password='password')
#         response = handler.lambda_handler(event, context)
#         os.environ['config_name'] = f'{os.path.join(project_root_path, config_file_name)}'
#
#         assert response['statusCode'] == 200
#         messages = kafka_helper.get_messages(bootstrap_servers=self.bootstrap_servers,
#                                              topic_name=topic_name,
#                                              security_protocol='SASL_PLAINTEXT',
#                                              sasl_mechanism='SCRAM-SHA-512',
#                                              username='client',
#                                              password='password')
#         print(messages)
#         print(response)
#         assert len(messages) == 1
#         assert messages[0]['header'] == 'tval1'
#         assert messages[0]['body'] == 'bval1'
#         assert messages[0]['footer'] == 'fval1'
