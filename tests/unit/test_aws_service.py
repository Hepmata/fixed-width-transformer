import pytest

from library import aws_service


def test_start_glue_crawler_success(mocker):
    mocker.patch.dict('os.environ', {'crawler_name': 'fake_crawler'})
    mocker.patch('boto3.client')
    aws_service.start_glue_crawler()


def test_start_glue_crawler_fail_noenvironvariable(mocker):
    mocker.patch.dict('os.environ', {})
    mocker.patch('boto3.client')
    with pytest.raises(Exception):
        aws_service.start_glue_crawler()


def test_download_s3_file_success_withnoneclient(mocker):
    mocker.patch('boto3.resource')
    filename = "/tmp/testfile"
    response = aws_service.download_s3_file(
        bucket="test",
        file_name=filename,
        s3_file_key="testkey"
    )
    assert response == filename
