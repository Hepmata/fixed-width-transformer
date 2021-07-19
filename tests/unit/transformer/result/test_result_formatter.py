import pandas as pd
from transformer.result import result_formatter

class TestResultFormatter:
    def test_single_segment(self):
        dataframes = {
            "header": pd.DataFrame({
                "field1": [1]
            }),
            "body": pd.DataFrame({
                "field1": [1, 2, 3, 4, 5],
                "field2": [6, 7, 8, 9, 10]
            })
        }

        cfg = result_formatter.ResultFormatterConfig(
            {
                "mapper": "DefaultArrayResultFormatter",
                "format": {
                    "body": [
                        {
                            "name": "one",
                            "value": "input.body.field1"
                        },
                        {
                            "name": "id",
                            "value": "UuidGenerator"
                        }
                    ]
                }
            }
        )
        results = result_formatter.DefaultArrayResultFormatter().run(cfg, dataframes)
        print(results)
    
    def test_2_segment(self):
        dataframes = {
            "header": pd.DataFrame({
                "field1": [1]
            }),
            "body": pd.DataFrame({
                "field1": [1, 2, 3, 4, 5],
                "field2": [6, 7, 8, 9, 10]
            })
        }

        cfg = result_formatter.ResultFormatterConfig(
            {
                "mapper": "DefaultArrayResultFormatter",
                "format": {
                    "metadata": [
                        {
                            "name": "field1Register",
                            "value": "input.header.field1"
                        }
                    ],
                    "body": [
                        {
                            "name": "one",
                            "value": "input.body.field1"
                        },
                        {
                            "name": "id",
                            "value": "UuidGenerator"
                        }
                    ]
                }
            }
        )
        results = result_formatter.DefaultArrayResultFormatter().run(cfg, dataframes)
        print(results)