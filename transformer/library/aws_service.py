from transformer.library import logger
import os
import boto3
import json
import botocore.exceptions as be

log = logger.set_logger(__name__)


def download_s3_file(bucket: str, key: str, file_name: str, client=None):
    log.info(f'Starting to download S3 file {key} from bucket [{bucket}] to local file [{file_name}]')
    if client is None:
        client = boto3.resource('s3')
    try:
        client.meta.client.download_file(bucket, key, file_name)
        log.info(f'File Downloaded!')
        return file_name
    except Exception as e:
        log.info("Failed to download s3 file!")
        log.error(e)


def upload_s3_file(file_name, bucket, s3_file_key, client=None):
    log.info(f'Starting to upload S3 file {s3_file_key} from bucket [{bucket}] from local file [{file_name}]')
    if client is None:
        client = boto3.resource('s3')
    try:
        response = client.meta.client.upload_file(file_name, bucket, s3_file_key)
        log.info(f'File Uploaded!')
        return response
    except Exception as e:
        log.info("Failed to upload s3 file!")
        log.error(e)


def download_s3_as_bytes(bucket: str, s3_key: str, client=None):
    log.info(f'Starting to download S3 file {s3_key} from bucket [{bucket}] to memory')
    if client is None:
        client = boto3.client('s3')
    response = client.get_object(Bucket=bucket, Key=s3_key)
    return response['Body']


def upload_s3_with_bytes(bucket: str, s3_key: str, bytes, client=None):
    log.info(f'Starting to upload S3 file {s3_key} from bucket [{bucket}] from memory')
    if client is None:
        client = boto3.client('s3')
    response = client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=bytes
    )
    log.info(response)
    log.info(f'File Uploaded!')


def retrieve_secret(secret_name: str):
    region_name = os.environ['region']
    session = boto3.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        return json.loads(client.get_secret_value(
            SecretId=secret_name
        )['SecretString'])
    except be.ClientError as e:
        print(e)
    raise Exception(f'Failed to retrieve secret [{secret_name}] from SecretsManager')