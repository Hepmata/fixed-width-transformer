from transformer.source import source_config


class TestSourceMapperConfig:
    def test_without_validators_and_converters(self):
        config_dict = {
            "pattern": "somepattern",
            "source": {
                "header": {
                    "mapper": "HeaderSourceFormatter",
                    "format": [
                        {
                            "name": "one",
                            "spec": "0,5"
                        }
                    ]
                }
            }
        }
        config = source_config.SourceMapperConfig(config_dict, "asd")
        print(config.get_converters())
        assert len(config.get_mappers()) == 1
        assert len(config.get_converters()) == 0
        assert len(config.get_validators()) == 0

    def test_with_validators(self):
        config_dict = {
            "pattern": "somepattern",
            "source": {
                "header": {
                    "mapper": "HeaderSourceFormatter",
                    "format": [
                        {
                            "name": "one",
                            "spec": "0,5",
                            "validators": [
                                {
                                    "name": "NricValidator"
                                }
                            ]
                        },
                        {
                            "name": "two",
                            "spec": "5,10",
                            "validators": [
                                {
                                    "name": "RegexValidator",
                                    "arguments": {
                                        "pattern": "^test$"
                                    }
                                }
                            ]
                        },
                        {
                            "name": "three",
                            "spec": "10,15"
                        }
                    ]
                }
            }
        }
        config = source_config.SourceMapperConfig(config_dict, "asd")
        assert len(config.get_mappers()) == 1
        assert len(config.get_converters()) == 0
        assert len(config.get_validators()) == 2

    def test_with_converters(self):
        config_dict = {
            "pattern": "somepattern",
            "source": {
                "header": {
                    "mapper": "HeaderSourceFormatter",
                    "format": [
                        {
                            "name": "one",
                            "spec": "0,5",
                            "converter": "StrConverter"
                        },
                        {
                            "name": "two",
                            "spec": "5,10",
                            "converter": "StrConverter"
                        }
                    ]
                },
                "body": {
                    "mapper": "BodySourceFormatter",
                    "format": [
                        {
                            "name": "one",
                            "spec": "0,5",
                            "converter": "StrConverter"
                        },
                        {
                            "name": "two",
                            "spec": "5,10"
                        }
                    ]
                }
            }
        }
        config = source_config.SourceMapperConfig(config_dict, "asd")
        assert len(config.get_mappers()) == 2
        assert len(config.get_converters()) == 3
        assert len(config.get_validators()) == 0

    def test_with_validators_and_converters(self):
        config_dict = {
            "pattern": "somepattern",
            "source": {
                "header": {
                    "mapper": "HeaderSourceFormatter",
                    "format": [
                        {
                            "name": "one",
                            "spec": "0,5",
                            "validators": [
                                {
                                    "name": "NricValidator"
                                }
                            ]
                        },
                        {
                            "name": "two",
                            "spec": "5,10",
                            "converter": "StrConverter",
                            "validators": [
                                {
                                    "name": "RegexValidator",
                                    "arguments": {
                                        "pattern": "^test$"
                                    }
                                }
                            ]
                        },
                        {
                            "name": "three",
                            "spec": "10,15"
                        }
                    ]
                }
            }
        }
        config = source_config.SourceMapperConfig(config_dict, "asd")
        assert len(config.get_mappers()) == 1
        assert len(config.get_converters()) == 1
        assert len(config.get_validators()) == 2
