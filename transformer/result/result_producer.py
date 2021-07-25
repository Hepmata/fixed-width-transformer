from transformer.result import ResultProducerConfig
from transformer.library import logger, aws_service
from transformer.library.kafka_service import connect_producer_with_url, connect_producer_with_cluster_name

log = logger.set_logger()


class AbstractResult:
    arguments: dict

    def __init__(self, config: ResultProducerConfig):
        self.arguments = config.arguments

    def run(self, data): pass


class S3ResultProducer(AbstractResult):
    def __init__(self, config: ResultProducerConfig):
        super().__init__(config)

    def run(self, data):
        aws_service.upload_s3_with_bytes(
            bucket=self.arguments['bucket'],
            s3_key=self.arguments['key'],
            bytes=data
        )


class MSKScramResultProducer(AbstractResult):
    def __init__(self, config: ResultProducerConfig):
        super().__init__(config)

    def run(self, data: list):
        if 'brokerUrls' in self.arguments.keys():
            producer = connect_producer_with_url(
                broker_urls=self.arguments['brokerUrls'],
                secret_name=self.arguments['secretName'],
                batch_size=self.arguments['batchSize']
            )
        else:
            producer = connect_producer_with_cluster_name(
                cluster_name=self.arguments['clusterName'],
                secret_name=self.arguments['secretName'],
                batch_size=self.arguments['batchSize']
            )

        for m in data:
            producer.send(
                topic=self.arguments['topic'],
                value=m
            )


class ConsoleResultProducer(AbstractResult):
    def __init__(self, config: ResultProducerConfig):
        super().__init__(config)

    def run(self, data):
        log.info("--- Printing Data in Console ---")
        if isinstance(data, list):
            for d in data:
                print(d)
        else:
            print(data)
        log.info("--- Print Completed ---")