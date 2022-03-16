import json
import random

from transformer.result import ResultProducerConfig
from transformer.library import logger, aws_service
from transformer.library.kafka_service import connect_producer
import threading

log = logger.set_logger()


class AbstractResult:
    arguments: dict

    def __init__(self, config: ResultProducerConfig):
        self.arguments = config.arguments

    def run(self, data):
        pass


class S3ResultProducer(AbstractResult):
    def __init__(self, config: ResultProducerConfig):
        super().__init__(config)

    def run(self, data):
        aws_service.upload_s3_with_bytes(
            bucket=self.arguments['bucket'], s3_key=self.arguments['key'], bytes=data
        )


class MSKScramResultProducer(AbstractResult):
    def __init__(self, config: ResultProducerConfig):
        super().__init__(config)

    def run(self, data: list):
        log.info('--- ResultProducer Logs ---')
        log.info(f'Producing {len(data)} messages to MSK')
        if 'ordered' in self.arguments.keys():
            if not self.arguments['ordered']:
                if (
                    len(data) > self.arguments['order_threshold']
                    if 'order_threshold' in self.arguments.keys()
                    else 100000
                ):
                    split_index = 4
                    threads = []
                    offset = 0
                    for itr in range(split_index):
                        end_index = int(len(data) / split_index * (itr + 1))
                        threads.append(
                            threading.Thread(
                                target=self.publish, args=(data[offset:end_index],)
                            )
                        )
                        offset = end_index
                    [t.start() for t in threads]
                    [t.join() for t in threads]
        else:
            self.publish(data)

        log.info('--- ResultProducer Completed ---')

    def publish(self, messages: list):
        producer = connect_producer(**self.arguments)
        for m in messages:
            producer.send(topic=self.arguments['topic'], value=m)
        producer.close()


class ConsoleResultProducer(AbstractResult):
    def __init__(self, config: ResultProducerConfig):
        super().__init__(config)

    def run(self, data):
        log.info('--- Printing Data in Console ---')
        if isinstance(data, list):
            for d in data:
                print(json.dumps(d))
        else:
            print(json.dumps(data))
        log.info('--- Print Completed ---')


class ConsoleMergedResultProducer(AbstractResult):
    def __init__(self, config: ResultProducerConfig):
        super().__init__(config)

    def run(self, data):
        log.info('--- Printing Data in Console ---')
        testData = data[0]
        temp_value = testData["body"]
        testData["body"] = [temp_value]
        if len(data) > 1:
            for i in range(1, len(data)):
                testData["body"].append(data[i]["body"])
            print(json.dumps(testData))
        log.info('--- Print Completed ---')


class SummaryResultProducer(AbstractResult):
    def __init__(self, config: ResultProducerConfig):
        super().__init__(config)

    def run(self, data):
        log.info('--- Summary Log ---')
        print(f'Number of Records: {len(data)}')
        sampling_record = data[random.randint(0, len(data))]
        print(f'Each Record has the following keys: {sampling_record.keys()}')
        print(f'Sample Record: {sampling_record}')
        log.info('--- End Summary Log ---')
