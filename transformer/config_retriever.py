import dataclasses
from typing import IO

from library import aws_service


class AbstractConfigRetriever:
    def __init__(self, **kwargs): pass
    def _retrieve(self) -> IO: pass


@dataclasses.dataclass
class S3ConfigRetriever(AbstractConfigRetriever):
    remote_bucket: str
    remote_key: str
    local_folder: str
    local_file_path: str

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.remote_bucket = kwargs['bucket']
        self.remote_key = kwargs['key']
        self.local_folder = kwargs['local_folder']
        stripped_filename = self.remote_key.split('/')
        self.local_file_path = f"{self.local_folder}/{stripped_filename}"

    def _retrieve(self) -> IO:
        return aws_service.download_s3_as_bytes(self.remote_bucket, self.remote_key)


@dataclasses.dataclass
class LocalConfigRetriever(AbstractConfigRetriever):
    local_file: str

    def __init__(self, **kwargs):
        super().__init__()
        """
        :param local_file: file name
        """
        self.local_file = kwargs['local_file']

    def _retrieve(self) -> IO:
        return open(self.local_file, 'r')
