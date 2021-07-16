
class ConfigError(Exception):
    pass


class MissingConfigError(Exception):
    pass


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
