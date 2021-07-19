from transformer.validator.validator_config import ValidatorConfig, ValidatorFieldConfig


class TestValidatorConfig:

    class TestSuccess:
        def test_with_arguments(self):
            f = ValidatorFieldConfig("test", {
                "someargs": "somevalue"
            })

            cfg = ValidatorConfig("test", "test", [f])

            assert len(cfg.validations) == 1
            assert cfg

        def test_without_arguments(self):
            f = ValidatorFieldConfig("test", {})
            cfg = ValidatorConfig("test", "test", [f])
            assert len(cfg.validations) == 1
            assert cfg

        def test_without_validations(self):
            cfg = ValidatorConfig("test", "test", [])
            assert len(cfg.validations) == 0
            assert cfg
