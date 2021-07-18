import io
import os
import uuid
from unittest.mock import MagicMock
import pytest
import yaml
from transformer.library import exceptions

from transformer.config import ExecutorConfig, ResultMapperConfig, SourceMapperConfig, ResultConfig


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

    def test_inline(self):
        config = ExecutorConfig(key=self.shared_good_key, inline=self.shared_good_config)
        assert config.get_exact_config()
        assert config.get_config()

    def test_local_relative_path(self):
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

    def test_local_abs_path(self):
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

    def test_local_env(self, mocker):
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

    def test_local_remote_env(self, mocker):
        mocker.patch.dict(os.environ, {"config_type": "external"})
        mocker.patch.dict(os.environ, {"config_bucket": "somebucket"})
        mocker.patch.dict(os.environ, {"config_name": "somekey"})
        mocker.patch("transformer.library.aws_service.download_s3_as_bytes", return_value=io.StringIO(self.shared_good_config))
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


class TestSourceMapperConfig:
    @pytest.fixture
    def executor_cfg(self):
        cfg = {
            "pattern": "somepattern",
            "source": {
                "header": {
                    "mapper":  "HeaderDataMapper",
                    "format": [
                        {
                            "name": "test",
                            "spec": "0,1",
                            "validators": [
                                {
                                    "name": "RegexValidator",
                                    "arguments": {
                                        "pattern": "somepattern"
                                    }
                                }
                            ]
                        },
                    ]
                }
            }
        }
        executor_config = MagicMock()
        executor_config.get_exact_config.return_value = cfg
        return executor_config

    def test_format(self, executor_cfg):
        result = SourceMapperConfig(executor_cfg)
        assert result.get_mappers()
        assert len(result.get_validations().keys()) == 1
        assert len(result.get_validations()['header']) == 1

    def test_no_validators(self, executor_cfg):
        cfg = {
            "pattern": "somepattern",
            "source": {
                "header": {
                    "mapper": "HeaderDataMapper",
                    "format": [
                        {
                            "name": "test",
                            "spec": "0,1",
                        }
                    ]
                }
            }
        }
        executor_cfg.get_exact_config.return_value = cfg
        result = SourceMapperConfig(executor_cfg)
        assert result.get_mappers()
        assert not result.get_validations()

    def test_empty_source(self, executor_cfg):
        cfg = {
            "pattern": "somepattern",
            "source": None
        }
        executor_cfg.get_exact_config.return_value = cfg
        with pytest.raises(exceptions.InvalidConfigError):
            SourceMapperConfig(executor_cfg)


class TestResultMapperConfig:
    @pytest.fixture
    def executor_cfg(self):
        cfg = {
                "pattern": "somepattern",
                "output": {
                    "result": {
                        "name": "some",
                        "arguments": {
                            "arg1": "",
                            "arg2": ""
                        },
                    },
                    "format": [
                        {
                            "name": "test",
                            "value": "lalala",
                            "validators": [
                                {
                                    "name": "RegexValidators",
                                    "arguments": {
                                        "pattern": "someval"
                                    }
                                }
                            ]
                        }
                    ]
                }
            }
        executor_config = MagicMock()
        executor_config.get_exact_config.return_value = cfg
        return executor_config

    def test_with_format(self, executor_cfg):
        cfg = ResultMapperConfig(executor_cfg)
        assert cfg.get_result_config()

    def test_no_format(self, executor_cfg):
        new_cfg = {
            "pattern": "somepattern",
            "output": {
                "result": {
                    "name": "somevalue"
                }
            }
        }
        executor_cfg.get_exact_config.return_value = new_cfg
        cfg = ResultMapperConfig(executor_cfg)
        assert not cfg.get_result_config()

    def test_no_result_content(self, executor_cfg):
        new_cfg = {
            "pattern": "somepattern",
            "output": {}
        }
        executor_cfg.get_exact_config.return_value = new_cfg
        cfg = ResultMapperConfig(executor_cfg)
        assert not cfg.get_result_config()

    def test_no_output(self, executor_cfg):
        new_cfg = {
            "pattern": "somepattern"
        }
        executor_cfg.get_exact_config.return_value = new_cfg
        with pytest.raises(exceptions.InvalidConfigError):
            ResultMapperConfig(executor_cfg)


class TestResultConfig:
    @pytest.fixture
    def executor_cfg(self):
        cfg = {
            "pattern": "somepattern",
            "output": {
                "result": {
                    "name": "some",
                    "arguments": {
                        "arg1": "",
                        "arg2": ""
                    },
                }
            }
        }
        executor_config = MagicMock()
        executor_config.get_exact_config.return_value = cfg
        return executor_config

    def test_success(self, executor_cfg):
        result = ResultConfig(executor_cfg)
        assert result.get_name()
        assert len(result.get_arguments().keys()) == 2

    def test_success_no_arguments(self, executor_cfg):
        cfg = {
            "pattern": "somepattern",
            "output": {
                "result": {
                    "name": "some"
                }
            }
        }
        executor_cfg.get_exact_config.return_value = cfg
        result = ResultConfig(executor_cfg)
        assert result.get_name()
        assert not result.get_arguments()

    def test_missing_result(self, executor_cfg):
        cfg = {
            "pattern": "somepattern",
            "output": {
                "result": None
            }
        }
        executor_cfg.get_exact_config.return_value = cfg
        with pytest.raises(exceptions.InvalidConfigError):
            ResultConfig(executor_cfg)

    def test_missing_result_empty(self, executor_cfg):
        cfg = {
            "pattern": "somepattern",
            "output": {
                "result": {}
            }
        }
        executor_cfg.get_exact_config.return_value = cfg
        with pytest.raises(exceptions.InvalidConfigError):
            ResultConfig(executor_cfg)

    def test_missing_result_entirely(self, executor_cfg):
        cfg = {
            "pattern": "somepattern",
            "output": {
            }
        }
        executor_cfg.get_exact_config.return_value = cfg
        with pytest.raises(exceptions.InvalidConfigError):
            ResultConfig(executor_cfg)