import datetime
import os
from os import listdir
from os.path import isfile, join

import pytest

from transformer.library import aws_service
from tests.helper import file_helper, aws_helper
from tests.helper.fixtures import test_id
from unittest.mock import patch, Mock


def teardown_module():
    files = [f for f in listdir('./') if isfile(join('./', f))]
    for file in files:
        if file.endswith('.txt') or file.endswith('.yml'):
            os.remove(file)


@patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
class TestS3FileCheck:
    def test_valid_s3_file_with_date_check(self, test_id, mocker):
        file_name = f'{test_id}.txt'
        bucket = 'integration-test'
        file_helper.create_file('test', file_name)
        aws_helper.upload_s3_file(file_name, bucket, file_name)
        results = aws_service.check_s3_file_exists(bucket, file_name, True)
        assert results is True

    def test_valid_s3_date_without_date_check(self, test_id, mocker):
        file_name = f'{test_id}.txt'
        bucket = 'integration-test'
        file_helper.create_file('test', file_name)
        aws_helper.upload_s3_file(file_name, bucket, file_name)
        results = aws_service.check_s3_file_exists(bucket, file_name, False)
        assert results is True

    def test_invalid_previous_date(self, test_id, mocker):
        file_name = f'{test_id}.txt'
        bucket = 'integration-test'
        file_helper.create_file('test', file_name)
        aws_helper.upload_s3_file(file_name, bucket, file_name)
        date = datetime.date.today()
        date = date - datetime.timedelta(days=1)
        mocked_s3_client = Mock()
        mocked_obj = Mock()
        mocked_obj.load.return_value = None
        mocked_obj.last_modified.return_value = date
        mocked_s3_client.Object.return_value = mocked_obj
        results = aws_service.check_s3_file_exists(bucket, file_name, True, mocked_s3_client)
        assert results is False

    def test_invalid_future_date(self, test_id, mocker):
        file_name = f'{test_id}.txt'
        bucket = 'integration-test'
        file_helper.create_file('test', file_name)
        aws_helper.upload_s3_file(file_name, bucket, file_name)
        date = datetime.date.today()
        date = date + datetime.timedelta(days=1)
        mocked_s3_client = Mock()
        mocked_obj = Mock()
        mocked_obj.load.return_value = None
        mocked_obj.last_modified.return_value = date
        mocked_s3_client.Object.return_value = mocked_obj
        results = aws_service.check_s3_file_exists(bucket, file_name, True, mocked_s3_client)
        assert results is False

    def test_invalid_no_file(self, test_id, mocker):
        file_name = f'{test_id}.txt'
        bucket = 'integration-test'
        results = aws_service.check_s3_file_exists(bucket, file_name)
        assert results is False


@patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
class TestS3FileDownload:
    def test_valid_s3_file_with_date_check(self, test_id, mocker):
        file_name = f'{test_id}.txt'
        bucket = 'integration-test'
        file_helper.create_file('test', file_name)
        aws_helper.upload_s3_file(file_name, bucket, file_name)
        aws_service.download_s3_file(bucket, file_name, f'{test_id}-dl.txt', True)
        assert os.path.exists(f'{test_id}-dl.txt')

    def test_valid_s3_file_without_date_check(self, test_id, mocker):
        file_name = f'{test_id}.txt'
        bucket = 'integration-test'
        file_helper.create_file('test', file_name)
        aws_helper.upload_s3_file(file_name, bucket, file_name)
        aws_service.download_s3_file(bucket, file_name, f'{test_id}-dl.txt', False)
        assert os.path.exists(f'{test_id}-dl.txt')

    def test_invalid_file_previous_date(self, test_id, mocker):
        file_name = f'{test_id}.txt'
        bucket = 'integration-test'
        file_helper.create_file('test', file_name)
        aws_helper.upload_s3_file(file_name, bucket, file_name)
        mocker.patch('transformer.library.aws_service.check_s3_file_exists', return_value=False)
        with pytest.raises(Exception):
            aws_service.download_s3_file(bucket, file_name, f'{test_id}-dl.txt', True)
