
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
    segment: str
    fieldName: str
    failCount: int
    recordCount: int

    def __init__(self, msg: str, segment: str, field_name: str, fail_count: int, record_count: int) -> None:
        self.segment = segment
        self.fieldName = field_name
        self.failCount = fail_count
        self.recordCount = record_count
        super().__init__(msg)


class SourceFileError(Exception):
    file_name: str

    def __init__(self, msg, file_name):
        self.file_name = file_name
        super().__init__(msg)