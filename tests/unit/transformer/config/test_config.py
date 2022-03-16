from transformer.source import source_config
from transformer.executor import executor_config
from yaml import safe_load


class TestConstraintConfig:
    def test_constraint(self):
        config_dict = {
            'pattern': 'somepattern',
            'constraints': [
                {
                    'name': 'HashConstraint',
                    'wait_time': '60s',
                    'arguments': {
                        'bucket': '',
                        'file_name': ''
                    }
                }
            ]
        }


class TestConfigLoading:
    def test_with_multiple_constraint(self):
        config_text = """
        files:
            test_file:
                pattern: ^somefile.txt$
                constraints:
                  - name: HashConstraint
                    max_wait_time: 60
                    wait_interval: 5
                    arguments:
                        bucket: some-bucket
                        file_name: some-hash-file.txt
                  - name: HashConstraint
                    max_wait_time: 60
                    wait_interval: 5
                    arguments:
                        bucket: some-bucket
                        file_name: some-hash-file.txt
                source:
                    header:
                        formatter: CSVHeaderFormatter
                        format:
                          - name: any
        """
        e_config = executor_config.ExecutorConfig(inline=config_text, key='somefile.txt')
        cfg = source_config.SourceMapperConfig(config=e_config.get_exact_config(), file_name='asd')
        print(cfg.get_constraints())
        constraints = cfg.get_constraints()
        assert len(constraints) == 2

    def test_with_single_constraint(self):
        config_text = """
        files:
            test_file:
                pattern: ^somefile.txt$
                constraints:
                    name: HashConstraint
                    max_wait_time: 60
                    wait_interval: 5
                    arguments:
                        bucket: some-bucket
                        file_name: some-hash-file.txt
                source:
                    header:
                        formatter: CSVHeaderFormatter
                        format:
                          - name: any
        """
        e_config = executor_config.ExecutorConfig(inline=config_text, key='somefile.txt')
        cfg = source_config.SourceMapperConfig(config=e_config.get_exact_config(), file_name='asd')
        print(cfg.get_constraints())
        constraints = cfg.get_constraints()
        assert len(constraints) == 1

    def test_with_no_constraints(self):
        config_text = """
        files:
            test_file:
                pattern: ^somefile.txt$
                source:
                    header:
                        formatter: CSVHeaderFormatter
                        format:
                          - name: any
        """
        e_config = executor_config.ExecutorConfig(inline=config_text, key='somefile.txt')
        cfg = source_config.SourceMapperConfig(config=e_config.get_exact_config(), file_name='asd')
        print(cfg.get_constraints())
        constraints = cfg.get_constraints()
        assert len(constraints) == 0
