import json
from transformer.library.aws_service import retrieve_secret
from transformer.library import logger
import boto3
from kafka import KafkaProducer

log = logger.set_logger(__name__)


def send_messages(data: list, cluster_name: str, topic: str, secret_name: str, batch_size: int, key=None):
    producer = __connect_producer__(
        cluster_name=cluster_name,
        secret_name=secret_name,
        batch_size=batch_size
    )
    if key is not None:
        producer.send(
            topic=topic,
            key=key,
            value=data
        )
    else:
        for m in data:
            producer.send(
                topic=topic,
                value=m
            )


def __connect_producer__(cluster_name: str, secret_name: str, batch_size: int, retries=3):
    bootstrap_servers = __retrieve_bootstrap_servers__(cluster_name)
    secret = retrieve_secret(secret_name)
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="default-client",
        request_timeout_ms=5000,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=secret['username'],
        sasl_plain_password=secret['password'],
        batch_size=batch_size,
        retries=retries,
        value_serializer=lambda m: json.dumps(m).encode('ascii')
    )


def __retrieve_cluster_arn__(cluster_name: str, client=None):
    if client is None:
        client = boto3.client('kafka')
    response = client.list_clusters(
        ClusterNameFilter=cluster_name,
        MaxResults=3
    )
    if len(response['ClusterInfoList']) == 0:
        raise Exception('No MSK Broker found. Please check cluster name in request')
    return response['ClusterInfoList'][0]['ClusterArn']


def __retrieve_bootstrap_servers__(cluster_name, client=None):
    if client is None:
        client = boto3.client('kafka')
    cluster_arn = __retrieve_cluster_arn__(cluster_name, client)
    return client.get_bootstrap_brokers(ClusterArn=cluster_arn)['BootstrapBrokerStringSaslScram']