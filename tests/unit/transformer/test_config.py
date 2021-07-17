import io
import os
import uuid
from library import aws_service
import pytest
import yaml
import library.exceptions as exceptions

from transformer.config import ExecutorConfig


class TestExecutorConfig:
    shared_good_key = "somefile.txt"
    shared_good_config = """
    files:
        somefile:
            pattern: ^somefile.txt$
            source:
                somekey: somevalue
            result:
                somekey: somevalue
    """

    def test_executor_config_inline(self):
        config = ExecutorConfig(key=self.shared_good_key, inline=self.shared_good_config)
        assert config.get_exact_config()
        assert config.get_config()

    def test_executor_config_local_relative_path(self):
        config_file = "config_{}.yaml".format(uuid.uuid4().__str__())
        config_text = yaml.load(self.shared_good_config)
        print(yaml.dump(config_text))
        with open(config_file, 'w') as f:
            yaml.safe_dump(config_text, f)
        print(os.path.abspath(config_file))
        config = ExecutorConfig(key=self.shared_good_key, local=config_file)
        os.remove(config_file)
        assert config.get_exact_config()
        assert config.get_config()

    def test_executor_config_local_absolute_path(self):
        config_file = "config_{}.yaml".format(uuid.uuid4().__str__())
        config_text = yaml.load(self.shared_good_config)
        print(yaml.dump(config_text))
        with open(config_file, 'w') as f:
            yaml.safe_dump(config_text, f)
        print(os.path.abspath(config_file))
        config = ExecutorConfig(key=self.shared_good_key, local=os.path.abspath(config_file))
        os.remove(config_file)
        assert config.get_exact_config()
        assert config.get_config()

    def test_executor_config_local(self, mocker):
        config_file = "config_{}.yaml".format(uuid.uuid4().__str__())
        print(os.path.abspath(config_file))
        mocker.patch.dict(os.environ, {"config_type": "local"})
        mocker.patch.dict(os.environ, {"config_name": config_file})
        config_text = yaml.load(self.shared_good_config)
        with open(config_file, 'w') as f:
            yaml.safe_dump(config_text, f)
        config = ExecutorConfig(key=self.shared_good_key)
        assert config.get_exact_config()
        assert config.get_config()
        os.remove(config_file)

    def test_executor_config_remote(self, mocker):
        mocker.patch.dict(os.environ, {"config_type": "external"})
        mocker.patch.dict(os.environ, {"config_bucket": "somebucket"})
        mocker.patch.dict(os.environ, {"config_name": "somekey"})
        mocker.patch("library.aws_service.download_s3_as_bytes", return_value=io.StringIO(self.shared_good_config))
        config = ExecutorConfig(key=self.shared_good_key)
        assert config.get_exact_config()
        assert config.get_config()

    def test_executor_cfg_invalid_local(self):
        with pytest.raises(exceptions.MissingConfigError):
            ExecutorConfig(key=self.shared_good_key, local="rubbish.file")

    def test_executor_cfg_invalid_inline(self):
        with pytest.raises(exceptions.InvalidConfigError):
            ExecutorConfig(key=self.shared_good_key, inline="rubbish")

    def test_executor_cfg_no_matching_key(self):
        bad_config = """
        files:
            somekey:
                pattern: ^\d+$
        """
        with pytest.raises(exceptions.MissingConfigError):
            ExecutorConfig(key=self.shared_good_key, inline=bad_config)

    def test_executor_cfg_no_keys(self):
        bad_config = """
        files:
        """
        with pytest.raises(exceptions.InvalidConfigError):
            ExecutorConfig(key=self.shared_good_key, inline=bad_config)