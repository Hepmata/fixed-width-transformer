import os
import uuid

import yaml

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
