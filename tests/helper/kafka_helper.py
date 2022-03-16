import json
from time import sleep

import kafka.errors
from kafka.admin import NewTopic, KafkaAdminClient
from kafka import KafkaConsumer


def create_topic(bootstrap_servers: str, topic_names, **security_args):
    if topic_names is None:
        raise ValueError('topic_names cannot be None or empty.')
    new_topics = []
    if type(topic_names) == list:
        for topic in topic_names:
            new_topics.append(NewTopic(
                name=topic,
                num_partitions=1,
                replication_factor=1
            ))
    elif type(topic_names) == str:
        new_topics.append(NewTopic(
            name=topic_names,
            num_partitions=1,
            replication_factor=1
        ))
    else:
        raise ValueError(f'topic_names can only be of str or list type, but received {type(topic_names)} instead')
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        security_protocol=security_args['security_protocol'],
        sasl_mechanism=security_args['sasl_mechanism'],
        sasl_plain_username=security_args['username'],
        sasl_plain_password=security_args['password'],
    )
    try:
        admin_client.create_topics(new_topics)
        sleep(1)
    except kafka.errors.TopicAlreadyExistsError:
        pass


def delete_topic(bootstrap_servers: str, topic_name: str):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin_client.delete_topics(topics=[topic_name])


def get_messages(bootstrap_servers: str, topic_name: str, **security_args):
    consumer = KafkaConsumer(
        topic_name,
        group_id='test-helper',
        bootstrap_servers=[bootstrap_servers],
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('ascii')),
        consumer_timeout_ms=1000,
        security_protocol=security_args['security_protocol'],
        sasl_mechanism=security_args['sasl_mechanism'],
        sasl_plain_username=security_args['username'],
        sasl_plain_password=security_args['password'],
        auto_offset_reset='earliest'
    )
    messages = []
    records = consumer.poll(5000)
    for tp, consumer_records in records.items():
        for consumer_record in consumer_records:
            messages.append(consumer_record.value)
    consumer.close()
    return messages
