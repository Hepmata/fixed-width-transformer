import json
from transformer.library.aws_service import retrieve_secret, retrieve_bootstrap_servers
from transformer.library import logger
from kafka import KafkaProducer

log = logger.set_logger(__name__)


def connect_producer_with_cluster_name(cluster_name: str, secret_name: str, batch_size: int, retries=3):
    bootstrap_servers = retrieve_bootstrap_servers(cluster_name)
    secret = retrieve_secret(secret_name)
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="default-client",
        request_timeout_ms=60000,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=secret['username'],
        sasl_plain_password=secret['password'],
        batch_size=batch_size,
        retries=retries,
        value_serializer=lambda m: json.dumps(m).encode('ascii')
    )


def connect_producer_with_url(broker_urls: str, secret_name: str, batch_size: int, retries=3):
    secret = retrieve_secret(secret_name)
    return KafkaProducer(
        bootstrap_servers=broker_urls,
        client_id="default-client",
        request_timeout_ms=60000,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=secret['username'],
        sasl_plain_password=secret['password'],
        batch_size=batch_size,
        retries=retries,
        value_serializer=lambda m: json.dumps(m).encode('ascii')
    )