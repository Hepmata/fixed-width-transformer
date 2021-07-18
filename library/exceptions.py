
class ConfigError(Exception):
    pass


class MissingConfigError(Exception):
    """
    Exception that's thrown when configuration is missing or the required segment is not found.
    """
    pass


class InvalidConfigError(Exception):
    """
    Exception that's thrown when a loaded yaml configuration cannot be parsed or read
    """
    def __init__(self, msg="Failed to parse data. Please ensure config format is valid"):
        super().__init__(msg)


class ProcessingError(Exception):
    pass


class FileError(Exception):
    pass


class AppenderError(Exception):
    pass


class ValidationError(Exception):
    pass
    # def __init__(self, message, count):
    #     super().__init__(message)
