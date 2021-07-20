import pandas as pd
from transformer.result import result_formatter


class TestResultFormatter:
    # def test_no_segment(self):
    #     dataframes = {
    #         "header": pd.DataFrame({
    #             "field1": [1]
    #         }),
    #         "body": pd.DataFrame({
    #             "field1": [1, 2, 3, 4, 5],
    #             "field2": [6, 7, 8, 9, 10]
    #         })
    #     }
    # 
    #     cfg = result_formatter.ResultFormatterConfig(
    #         {
    #             "mapper": "DefaultArrayResultFormatter",
    #             "format": {
    #                 "body": [
    #                     {
    #                         "name": "one",
    #                         "value": "input.body.field1"
    #                     },
    #                     {
    #                         "name": "id",
    #                         "value": "UuidGenerator"
    #                     }
    #                 ]
    #             }
    #         }
    #     )
    #     results = result_formatter.DefaultArrayResultFormatter().run(cfg, dataframes)
    #     print(results)
    #     assert isinstance(results, list)
    #     assert len(results) == 5
    #     assert len(results[0].keys()) == 2
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
        assert isinstance(results, list)
        assert len(results) == 5
        assert "body" in results[0].keys() and len(results[0].keys()) == 1
        assert "one" in results[0]['body'].keys()
        assert "id" in results[0]['body'].keys()

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
                        },
                        {
                            "name": "batchId",
                            "value": "UuidGenerator"
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
        assert isinstance(results, list)
        assert len(results) == 5
        keys = results[0].keys()
        assert len(keys) == 2
        assert "metadata" in keys
        assert "body" in keys

    def test_no_provided_format(self):
        dataframes = {
            "header": pd.DataFrame({
                "field1": [1]
            }),
            "body": pd.DataFrame({
                "field1": [1, 2, 3, 4, 5],
                "field2": [6, 7, 8, 9, 10]
            }),
            "footer": pd.DataFrame({
                "specialfield": [1,2,3,4,5]
            })
        }

        cfg = result_formatter.ResultFormatterConfig(
            {
                "mapper": "DefaultArrayResultFormatter",
            }
        )
        results = result_formatter.DefaultArrayResultFormatter().run(cfg, dataframes)
        print(results)