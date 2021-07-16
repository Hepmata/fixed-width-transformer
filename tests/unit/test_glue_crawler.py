from src.glue_crawler_s3_trigger import handler

def test_handler_success(mocker):
    mocker.patch('library.aws_service.start_glue_crawler')
    handler.lambda_handler("", "")