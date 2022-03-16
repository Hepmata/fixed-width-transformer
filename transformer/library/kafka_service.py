import json
from transformer.library.aws_service import retrieve_secret, retrieve_bootstrap_servers
from transformer.library import logger
from kafka import KafkaProducer

log = logger.set_logger(__name__)


def connect_producer(**kwargs):
    keys = kwargs.keys()
    bootstrap_servers = (
        kwargs['broker_urls']
        if 'broker_urls' in keys
        else retrieve_bootstrap_servers(kwargs['cluster_name'])
    )
    secret = retrieve_secret(kwargs['secret_name'])
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id='default-client' if 'client_id' not in keys else kwargs['client_id'],
        request_timeout_ms=30000
        if 'request_timeout_ms' not in keys
        else kwargs['request_timeout_ms'],
        security_protocol='SASL_SSL' if 'security_protocol' not in keys else kwargs['security_protocol'],
        sasl_mechanism='SCRAM-SHA-512' if 'sasl_mechanism' not in keys else kwargs['sasl_mechanism'],
        sasl_plain_username=secret['username'],
        sasl_plain_password=secret['password'],
        batch_size=1000 if 'batch_size' not in keys else kwargs['batch_size'],
        retries=3 if 'retries' not in keys else kwargs['retries'],
        linger_ms=0 if 'linger_ms' not in keys else kwargs['linger_ms'],
        compression_type=None
        if 'compression_type' not in keys
        else kwargs['compression_type'],
        value_serializer=lambda m: json.dumps(m).encode('ascii'),
    )
