from transformer.library import aws_service


def test_download_s3_file_success_no_client(mocker):
    mocker.patch('boto3.resource')
    filename = "/tmp/testfile"
    response = aws_service.download_s3_file(
        bucket="test",
        file_name=filename,
        key="testkey"
    )
    assert response == filename
