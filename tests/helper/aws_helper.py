import dataclasses
import datetime
import json
import os

import boto3


def check_secret_exists(key: str):
    session = boto3.Session()
    client = session.client(
        service_name='secretsmanager', endpoint_url=os.environ['AWS_ENDPOINT_URL']
    )
    response = client.list_secrets(
        MaxResults=1, Filters=[{'Key': 'name', 'Values': [key]}]
    )
    return True if len(response['SecretList']) > 0 else False


def create_secret(secret_name: str, content: dict, request_token: str):
    session = boto3.Session()
    client = session.client(
        service_name='secretsmanager', endpoint_url=os.environ['AWS_ENDPOINT_URL']
    )
    if check_secret_exists(secret_name):
        pass
    else:
        client.create_secret(
            Name=secret_name,
            ClientRequestToken=request_token,
            SecretString=json.dumps(content),
        )


def create_s3_bucket(bucket: str, client=None):
    if client is None:
        client = boto3.resource('s3', endpoint_url='http://localhost:4566')
    client.Bucket(bucket).create()


def upload_s3_file(file_name, bucket, s3_file_key, client=None):
    if client is None:
        client = boto3.resource('s3', endpoint_url='http://localhost:4566')
    create_s3_bucket(bucket=bucket, client=client)
    return client.Bucket(bucket).upload_file(file_name, s3_file_key)


def create_event(bucket_name: str, s3_key: str):
    return {
        'Records': [{
            'eventSource': 'aws:s3',
            'awsRegion': 'ap-southeast-1',
            'eventTime': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            'eventName': 'ObjectCreated:Put',
            'userIdentity': {
                'principalId': 'integration_user'
            },
            's3': {
                's3SchemaVersion': '1.0',
                'configurationId': 'FixedWidthTransformer-qa-FixedWidthTransformer',
                'bucket': {
                    'name': bucket_name,
                    'arn': f'arn:aws:s3:::{bucket_name}'
                },
                'object': {
                    'key': s3_key
                }
            }
        }]
    }


@dataclasses.dataclass
class MockContext:
    aws_request_id: str
    log_group_name: str = dataclasses.field(default='')
