import os
import pytest
import pandas as pd
from transformer.library import common


class TestBuildAwsEndpointUrl:

    def test_without_service(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'ap-southeast-1'})
        url = common.build_aws_endpoint_url()
        assert url == 'https://ap-southeast-1.amazonaws.com'

    def test_with_service_s3(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'ap-southeast-1'})
        url = common.build_aws_endpoint_url('s3')
        assert url == 'https://s3.ap-southeast-1.amazonaws.com'

    def test_with_service_secretsmanager(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'ap-southeast-1'})
        url = common.build_aws_endpoint_url('secretsmanager')
        assert url == 'https://secretsmanager.ap-southeast-1.amazonaws.com'

    def test_with_service_kafka(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'ap-southeast-1'})
        url = common.build_aws_endpoint_url('kafka')
        assert url == 'https://kafka.ap-southeast-1.amazonaws.com'

    def test_with_service_lambda(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'ap-southeast-1'})
        url = common.build_aws_endpoint_url('lambda')
        assert url == 'https://lambda.ap-southeast-1.amazonaws.com'

    def test_invalid_region(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'asdasd'})
        with pytest.raises(ValueError):
            url = common.build_aws_endpoint_url('s3')


class TestPandasDiff:
    def test_get_failed_record_count(self):
        source_df = pd.Series(['1', '2', '3', '4'])
        ref_df = pd.Series(['2', '2', '3', '4'])

        result = common.get_failed_record_count(source_df, ref_df)
        assert result == 1


class TestConvertSqsArnToUrl:
    def test_valid(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'ap-southeast-1'})
        arn = 'arn:aws:sqs:ap-southeast-1:1234:test'
        results = common.convert_sqs_arn_to_url(arn)
        assert results == f"{common.build_aws_endpoint_url('sqs')}/1234/test"

    def test_invalid(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'ap-southeast-1'})
        arn = 'test'
        with pytest.raises(ValueError):
            common.convert_sqs_arn_to_url(arn)

    def test_null(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'ap-southeast-1'})
        with pytest.raises(ValueError):
            common.convert_sqs_arn_to_url(None)

    def test_empty(self, mocker):
        mocker.patch.dict(os.environ, {'region': 'ap-southeast-1'})
        with pytest.raises(ValueError):
            common.convert_sqs_arn_to_url('')
