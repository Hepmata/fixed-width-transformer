import uuid

import pandas as pd
from transformer.result import (
    ResultFormatterConfig,
    ResultFieldFormat,
    DefaultArrayResultFormatter,
)


class TestResultFormatter:
    def test_no_segment(self):
        dataframes = {
            'header': pd.DataFrame({'field1': [1]}),
            'body': pd.DataFrame(
                {
                    'field1': [1, 2, 3, 4, 5],
                    'field2': [6, 7, 8, 9, 10],
                    'field3': ['One', 'Two', 'Three', 'Four', 'Five'],
                }
            ),
        }
        cfg = ResultFormatterConfig(
            name='DefaultArrayResultFormatter',
            formats={
                'root': [
                    ResultFieldFormat('id', 'UuidGenerator'),
                    ResultFieldFormat('value', 'body.field1'),
                    ResultFieldFormat('descriptor', 'body.field3'),
                    ResultFieldFormat('crossReferenceValue', 'header.field1'),
                ]
            },
        )
        formatter = DefaultArrayResultFormatter()
        prepared_data = formatter.prepare(cfg, dataframes)
        final_results = formatter.transform(prepared_data)
        print(final_results)
        assert isinstance(final_results, list)
        assert len(final_results) == 5

    def test_single_segment(self):
        dataframes = {
            'header': pd.DataFrame({'field1': [1]}),
            'body': pd.DataFrame(
                {
                    'field1': [1, 2, 3, 4, 5],
                    'field2': [6, 7, 8, 9, 10],
                    'field3': ['One', 'Two', 'Three', 'Four', 'Five'],
                }
            ),
        }
        cfg = ResultFormatterConfig(
            name='DefaultArrayResultFormatter',
            formats={
                'body': [
                    ResultFieldFormat('id', 'UuidGenerator'),
                    ResultFieldFormat('value', 'body.field1'),
                    ResultFieldFormat('descriptor', 'body.field3'),
                    ResultFieldFormat('crossReferenceValue', 'header.field1'),
                ]
            },
        )
        formatter = DefaultArrayResultFormatter()
        prepared_data = formatter.prepare(cfg, dataframes)
        final_results = formatter.transform(prepared_data)
        print(final_results)
        assert isinstance(final_results, list)
        assert len(final_results) == 5
        assert 'body' in final_results[0].keys() and len(final_results[0].keys()) == 1
        keys = final_results[0]['body'].keys()
        for fmt in cfg.formats['body']:
            assert fmt.name in keys

    def test_cross_reference(self):
        dataframes = {
            'header': pd.DataFrame({'id': [uuid.uuid4().__str__()]}),
            'footer': pd.DataFrame(
                {'weirdId': [1], 'weirdDescriptor': ['Hey It works!']}
            ),
        }
        cfg = ResultFormatterConfig(
            name='DefaultArrayResultFormatter',
            formats={
                'metadata': [
                    ResultFieldFormat('intId', 'IdGenerator'),
                    ResultFieldFormat('batchId', 'header.id'),
                    ResultFieldFormat('weirdId', 'footer.weirdId'),
                    ResultFieldFormat('duplicatedWeirdId', 'footer.weirdId'),
                    ResultFieldFormat('weirdDescriptor', 'footer.weirdDescriptor'),
                ]
            },
        )
        formatter = DefaultArrayResultFormatter()
        prepared_data = formatter.prepare(cfg, dataframes)
        final_results = formatter.transform(prepared_data)
        print(final_results)

        assert isinstance(final_results, list)
        assert len(final_results) == 1
        assert (
            'metadata' in final_results[0].keys() and len(final_results[0].keys()) == 1
        )
        keys = final_results[0]['metadata'].keys()
        for fmt in cfg.formats['metadata']:
            assert fmt.name in keys

    def test_2_segment(self):
        dataframes = {
            'header': pd.DataFrame({'field1': [1]}),
            'body': pd.DataFrame(
                {'field1': [1, 2, 3, 4, 5], 'field2': [6, 7, 8, 9, 10]}
            ),
        }
        cfg = ResultFormatterConfig(
            name='DefaultArrayResultFormatter',
            formats={
                'metadata': [
                    ResultFieldFormat('field1Register', 'header.field1'),
                    ResultFieldFormat('batchId', 'UuidGenerator'),
                ],
                'body': [
                    ResultFieldFormat('one', 'body.field1'),
                    ResultFieldFormat('id', 'UuidGenerator'),
                ],
            },
        )

        formatter = DefaultArrayResultFormatter()
        prepared_data = formatter.prepare(cfg, dataframes)
        final_results = formatter.transform(prepared_data)
        print(final_results)

    def test_no_provided_format(self):
        dataframes = {
            'header': pd.DataFrame({'field1': [1]}),
            'body': pd.DataFrame(
                {'field1': [1, 2, 3, 4, 5], 'field2': [6, 7, 8, 9, 10]}
            ),
            'footer': pd.DataFrame({'specialfield': [1]}),
        }

        cfg = ResultFormatterConfig(name='DefaultArrayResultFormatter', formats={})
        formatter = DefaultArrayResultFormatter()
        prepared_data = formatter.prepare(cfg, dataframes)
        final_results = formatter.transform(prepared_data)
        print(final_results)
