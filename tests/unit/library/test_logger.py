import logging

from library import logger


class TestLogger:
    def test_logger_with_name(self, mocker):
        levels = [
            {'name': 'info', 'actual': logging.INFO},
            {'name': 'warn', 'actual': logging.WARN},
            {'name': 'debug', 'actual': logging.DEBUG},
            {'name': 'error', 'actual': logging.ERROR}
        ]
        for level in levels:
            mocker.patch.dict('os.environ', {'log_level': level['name']})
            log = logger.set_logger(TestLogger.__name__)
            assert log.level == level['actual']

    def test_logger_without_name(self, mocker):
        levels = [
            {'name': 'info', 'actual': logging.INFO},
            {'name': 'warn', 'actual': logging.WARN},
            {'name': 'debug', 'actual': logging.DEBUG},
            {'name': 'error', 'actual': logging.ERROR}
        ]
        for level in levels:
            mocker.patch.dict('os.environ', {'log_level': level['name']})
            log = logger.set_logger()
            assert log.level == level['actual']

    def test_logger_caps(self, mocker):
        levels = [
            {'name': 'INFO', 'actual': logging.INFO},
            {'name': 'WARN', 'actual': logging.WARN},
            {'name': 'DEBUG', 'actual': logging.DEBUG},
            {'name': 'ERROR', 'actual': logging.ERROR}
        ]
        for level in levels:
            mocker.patch.dict('os.environ', {'log_level': level['name']})
            log = logger.set_logger()
            assert log.level == level['actual']

    def test_logger_invalid_name(self, mocker):
        mocker.patch.dict('os.environ', {'log_level': 'kasdlalsd'})
        log = logger.set_logger(TestLogger.__name__)
        assert log
        assert log.level == logging.INFO