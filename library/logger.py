import logging
import os


def set_logger(name=None) -> logging:
    if name is not None:
        logger = logging.getLogger(name)
    else:
        logger = logging.getLogger()

    if 'log_level' in os.environ.keys():
        log_level = os.environ['log_level'].lower()
        if log_level == "warn":
            logger.setLevel(logging.WARN)
        elif log_level == "debug":
            logger.setLevel(logging.DEBUG)
        elif log_level == "error":
            logger.setLevel(logging.ERROR)
        else:
            logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.INFO)

    return logger
