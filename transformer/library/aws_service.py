import datetime
import time

import botocore.exceptions
from botocore.config import Config
from transformer.library import logger, common
import os
import boto3
import json
import botocore.exceptions as be
from tzlocal import get_localzone

log = logger.set_logger(__name__)


def check_s3_file_exists(bucket, s3_file_key, date_check=False, client=None):
    if client is None:
        client = boto3.resource('s3', endpoint_url=common.build_aws_endpoint_url('s3'))
    try:
        obj = client.Object(bucket, s3_file_key)
        obj.load()
        if date_check:
            current_date = datetime.datetime.today()
            if current_date.date() != obj.last_modified.date():
                return False
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise e
    return True


def upload_s3_file(file_name, bucket, s3_file_key, client=None):
    log.info(
        f'Starting to upload S3 file {s3_file_key} from bucket [{bucket}] from local file [{file_name}]'
    )
    if client is None:
        client = boto3.resource('s3', endpoint_url=common.build_aws_endpoint_url('s3'))
    try:
        response = client.meta.client.upload_file(file_name, bucket, s3_file_key)
        log.info(f'File Uploaded!')
        return response
    except Exception as e:
        log.info('Failed to upload s3 file!')
        log.error(e)


def upload_s3_with_bytes(bucket: str, s3_key: str, bytes, client=None):
    log.info(f'Starting to upload S3 file {s3_key} from bucket [{bucket}] from memory')
    if client is None:
        client = boto3.client('s3')
    response = client.put_object(Bucket=bucket, Key=s3_key, Body=bytes)
    log.info(response)
    log.info(f'File Uploaded!')


def download_s3_file(bucket: str, key: str, file_name: str, date_check=False, client=None):
    log.info(
        f'Starting to download S3 file {key} from bucket [{bucket}] to local file [{file_name}]'
    )
    if client is None:
        client = boto3.resource('s3', endpoint_url=common.build_aws_endpoint_url('s3'))
    if date_check:
        if not check_s3_file_exists(bucket, key, True, client):
            raise Exception(
                f'File {key} with matching date {datetime.datetime.today().strftime("%d-%m-%Y")} could not be found.')
    client.meta.client.download_file(bucket, key, file_name)
    log.info(f'File Downloaded!')
    return file_name


def download_s3_as_bytes(bucket: str, s3_key: str, date_check=False, client=None):
    log.info(f'Starting to download S3 file {s3_key} from bucket [{bucket}] to memory')
    if client is None:
        client = boto3.client('s3', endpoint_url=common.build_aws_endpoint_url('s3'))
    if date_check:
        if not check_s3_file_exists(bucket, s3_key, True, client):
            raise Exception(f'File {s3_key} with matching date {datetime.datetime.today().strftime("%d-%m-%Y")} could not be found.')
    response = client.get_object(Bucket=bucket, Key=s3_key)
    return response['Body']


def retrieve_secret(secret_name: str):
    region_name = os.environ['region']
    session = boto3.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name, endpoint_url=common.build_aws_endpoint_url('secretsmanager'))
    try:
        return json.loads(client.get_secret_value(SecretId=secret_name)['SecretString'])
    except be.ClientError as e:
        print(e)
    raise Exception(f'Failed to retrieve secret [{secret_name}] from SecretsManager')


def retrieve_cluster_arn(cluster_name: str, client=None):
    if client is None:
        client = boto3.client('kafka')
    response = client.list_clusters(ClusterNameFilter=cluster_name, MaxResults=3)
    if len(response['ClusterInfoList']) == 0:
        raise Exception('No MSK Broker found. Please check cluster name in request')
    return response['ClusterInfoList'][0]['ClusterArn']


def retrieve_bootstrap_servers(cluster_name, client=None):
    if client is None:
        client = boto3.client('kafka', endpont_url=common.build_aws_endpoint_url('kafka'))
    cluster_arn = retrieve_cluster_arn(cluster_name, client)
    return client.get_bootstrap_brokers(ClusterArn=cluster_arn)[
        'BootstrapBrokerStringSaslScram'
    ]


def invoke_lambda(function_name: str, payload: str, client=None):
    if client is None:
        cfg = Config(
            retries={'max_attempts': 2},
            connect_timeout=60,
            read_timeout=60
        )
        client = boto3.client('lambda', config=cfg, endpoint_url=common.build_aws_endpoint_url('lambda'))
    log.info(f'Invoking Lambda {function_name}...')
    response = client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=payload,
    )
    method_response = json.loads(response['Payload'].read())
    response['Payload'].close()
    log.info(f'Successfully invoked Lambda {function_name}.')
    log.info(method_response)
    return method_response


def create_sqs_message(url: str, message, client=None):
    if client is None:
        client = boto3.client('sqs', endpoint_url=common.build_aws_endpoint_url('sqs'))
    if type(message) == list or type(message) == dict:
        message = json.dumps(message)
    if 'arn' in url:
        url = common.convert_sqs_arn_to_url(url)
    return client.send_message(
        QueueUrl=url,
        MessageBody=message,
    )


def delete_sqs_message(url: str, recipe_handle: str, client=None):
    if client is None:
        client = boto3.client('sqs', endpoint_url=common.build_aws_endpoint_url('sqs'))
    return client.delete_message(
        QueueUrl=url,
        ReceiptHandle=recipe_handle
    )


def retrieve_lambda_statistics(function_name: str, request_id: str, client=None):
    if client is None:
        client = boto3.client('logs', endpoint_url=common.build_aws_endpoint_url('logs'))

    log.info('Waiting 10 seconds for lambda logs to propagate')
    timenow = datetime.datetime.now(get_localzone())
    query_string = f'fields @timestamp, @message, @maxMemoryUsed, @memorySize, @initDuration, @duration | filter @message like "REPORT RequestId: {request_id}" | limit 1'
    response = client.start_query(
        logGroupName=f'/aws/lambda/{function_name}',
        startTime=int((timenow - datetime.timedelta(1)).timestamp()),
        endTime=int(timenow.timestamp()),
        queryString=query_string,
        limit=1,
    )
    query_id = response['queryId']
    results = None
    while results is None or results['status'] == 'Running':
        log.info('Waiting for query to complete ...')
        time.sleep(1)
        results = client.get_query_results(queryId=query_id)
    if results['statistics']['recordsMatched'] == 0.0:
        log.error(
            f'No results found for requestId: {request_id} in queryId: {query_id}. This is a bug!'
        )
        return {
            'initDuration': 0,
            'maxMemoryUsed': 0,
            'duration': 0,
            'memorySize': 0,
        }
    results_data = {}
    for m in results['results'][0]:
        results_data[m['field'][1:]] = m['value']

    return results_data
