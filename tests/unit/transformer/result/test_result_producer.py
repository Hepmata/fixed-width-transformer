import pytest
from transformer.result import result_producer, ResultProducerConfig
from unittest import mock


class TestConsoleResultProducer:
    @pytest.fixture
    def pre_config(self):
        return {
            "result": {
                "producer": {
                    "name": "ConsoleResultProducer"
                }
            }
        }

    def test_array(self, pre_config, capsys):
        config = ResultProducerConfig(pre_config)
        producer = result_producer.ConsoleResultProducer(config)
        producer.run([{"Field1": "Works"}, {"Field1": "Works as Well!"}])
        captured = capsys.readouterr()
        print(captured)
        assert len(captured.out.split("\n")) == 3

    def test_dict(self, pre_config, capsys):
        config = ResultProducerConfig(pre_config)
        producer = result_producer.ConsoleResultProducer(config)
        producer.run({"Field1": "Works!"})
        captured = capsys.readouterr()
        print(captured)
        assert len(captured.out.split("\n")) == 2

    def test_str(self, pre_config, capsys):
        config = ResultProducerConfig(pre_config)
        producer = result_producer.ConsoleResultProducer(config)
        producer.run("Simple String")
        captured = capsys.readouterr()
        print(captured)
        assert len(captured.out.split("\n")) == 2


class TestS3ResultProducer:
    @pytest.fixture
    def pre_config(self):
        return {
            "result": {
                "producer": {
                    "name": "S3ResultProducer",
                    "arguments": {
                        "bucket": "somebucket",
                        "key": "somekey.txt"
                    }
                }
            }
        }

    def test_produce_results(self, mocker, pre_config):
        mocker.patch('transformer.library.aws_service.upload_s3_with_bytes')
        config = ResultProducerConfig(pre_config)
        producer = result_producer.S3ResultProducer(config)
        producer.run({"Field1": "Works"})


# class TestMSKScramResultProducer:
#     @pytest.fixture
#     def pre_config(self):
#         return {
#             "result": {
#                 "producer": {
#                     "name": "S3ResultProducer",
#                     "arguments": {
#                         "clusterName": "testName",
#                         "secretName": "testSecret",
#                         "batchSize": 40,
#                         "topic": "TestTopic"
#                     }
#                 }
#             }
#         }
#     
#     @mock.patch('transformer.library.kafka_service')
#     @mock.patch('transformer.library.aws_service')
#     def test_produce_results(self, kafka_service, aws_service, pre_config):
#         # mocker.patch('.connect_producer_with_cluster_name')
#         # mocker.patch('')
#         config = ResultProducerConfig(pre_config)
#         print(config)
#         producer = result_producer.MSKScramResultProducer(config)
#         producer.run([{"test", "value"}, {"test2", "value"}])
