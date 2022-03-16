import os
import uuid

import numpy as np
import pytest
from transformer.library.exceptions import ValidationError, MissingConfigError
import pandas as pd
from transformer.validator import validator
import transformer.library.aws_service as aws_service
from unittest import mock
from unittest.mock import MagicMock


class TestNaNValidator:
    @pytest.fixture
    def dataframes(self):
        return {'body': pd.DataFrame({'field': ['1', 'asdasd', 'asdasd']})}

    class TestSuccess:
        def test_all(self, dataframes):
            validator.NaNValidator().validate('body', 'ALL', {}, dataframes)

        def test_field(self, dataframes):
            validator.NaNValidator().validate('body', 'field', {}, dataframes)

    class TestFailure:
        def test_failure_all(self, dataframes):
            dataframes['body'] = pd.DataFrame(
                {'field': ['1', np.NAN, 'asdasd'], 'field2': [1, 2, 3]}
            )
            with pytest.raises(ValidationError):
                validator.NaNValidator().validate('body', 'ALL', {}, dataframes)

        def test_failure_field(self, dataframes):
            dataframes['body'] = pd.DataFrame(
                {'field': ['1', np.NAN, 'asdasd'], 'field2': [1, 2, 3]}
            )
            with pytest.raises(ValidationError):
                validator.NaNValidator().validate('body', 'field', {}, dataframes)


class TestRefValidator:
    class TestSuccess:
        def test_count(self):
            dataframes = {
                'body': pd.DataFrame({'field1': [1, 2, 3, 4, 5]}),
                'footer': pd.DataFrame({'recordCount': [5]}),
            }
            arguments = {'type': 'count', 'ref': 'footer.recordCount'}
            validator.RefValidator().validate('body', 'field1', arguments, dataframes)

        def test_count_reverse(self):
            dataframes = {
                'body': pd.DataFrame({'field1': [1, 2, 3, 4, 5]}),
                'footer': pd.DataFrame({'recordCount': [5]}),
            }
            arguments = {'type': 'count', 'ref': 'body.field1'}
            validator.RefValidator().validate(
                'footer', 'recordCount', arguments, dataframes
            )

        def test_match(self):
            dataframes = {
                'body': pd.DataFrame(
                    {'field1': [1, 2, 3, 4, 5], 'matcherField': [1, 2, 3, 4, 5]}
                ),
                'footer': pd.DataFrame({'recordCount': [5]}),
            }

            arguments = {'type': 'match', 'ref': 'body.matcherField'}
            validator.RefValidator().validate('body', 'field1', arguments, dataframes)

        def test_match_reverse(self):
            dataframes = {
                'body': pd.DataFrame(
                    {
                        'field1': [1, 2, 3, 4, 5],
                    }
                ),
                'footer': pd.DataFrame({'recordCount': [5]}),
                'match': pd.DataFrame({'field1': [1, 2, 3, 4, 5]}),
            }
            arguments = {'type': 'match', 'ref': 'match.field1'}
            validator.RefValidator().validate('body', 'field1', arguments, dataframes)

    class TestFailure:
        def test_failure(self):
            dataframes = {
                'body': pd.DataFrame({'field1': [1, 2, 3, 4, 5]}),
                'footer': pd.DataFrame({'recordCount': [2]}),
            }
            arguments = {'type': 'count', 'ref': 'body.field1'}
            with pytest.raises(ValidationError):
                validator.RefValidator().validate(
                    'footer', 'recordCount', arguments, dataframes
                )


class TestRegexValidator:
    @pytest.fixture
    def dataframes(self):
        return {'footer': pd.DataFrame({'field1': ['SX1', 'SX2', 'SX3', 'SX4', 'SX5']})}

    def test_regex(self, dataframes):
        arguments = {'pattern': r'^SX\d+$'}
        validator.RegexValidator().validate('footer', 'field1', arguments, dataframes)

    def test_invalid_pattern(self, dataframes):
        arguments = {'pattern': 'asdasda'}
        with pytest.raises(ValidationError):
            validator.RegexValidator().validate(
                'footer', 'field1', arguments, dataframes
            )

    def test_missing_argument(self, dataframes):
        with pytest.raises(MissingConfigError):
            validator.RegexValidator().validate('footer', 'field1', {}, dataframes)

    def test_invalid_argument_type(self, dataframes):
        arguments = {'pattern': 0}
        with pytest.raises(MissingConfigError):
            validator.RegexValidator().validate(
                'footer', 'field1', arguments, dataframes
            )

    def test_mismatch(self, dataframes):
        arguments = {'pattern': r'^SX$'}
        with pytest.raises(ValidationError):
            validator.RegexValidator().validate(
                'footer', 'field1', arguments, dataframes
            )


class TestSqlValidator:

    @pytest.fixture
    def dataframes(self):
        return {'body': pd.DataFrame({'name': ['john']})}

    @pytest.fixture
    def arguments(self):
        return {
            'secret_name': 'test/secret',
            'host': 'localhost:3306',
            'database': 'test_db',
            'sql_language': 'MySQL',
            'sql_query': "SELECT name from db.names WHERE name = 'JOHN' LIMIT 1;",
        }
    def test_success_sql_1to1(self, mocker, dataframes, arguments):
        mocker.patch(
            'transformer.library.aws_service.retrieve_secret',
            return_value={'username': 'test', 'password': 'test'},
        )
        mocker.patch('mysql.connector.connect')
        mocker.patch('pandas.read_sql', return_value=pd.DataFrame({'name': ['john']}))
        validator.SqlValidator().validate(
            segment='body', field_name='name', arguments=arguments, frames=dataframes
        )
    def test_success_sql_many_to_many(self, mocker, dataframes, arguments):
        dataframes['body'] = pd.DataFrame({
            'name': ['john', 'mcclane'],
            'age': [50,60]
        })
        mocker.patch(
            'transformer.library.aws_service.retrieve_secret',
            return_value={'username': 'test', 'password': 'test'},
        )
        mocker.patch('mysql.connector.connect')
        mocker.patch('pandas.read_sql', return_value=pd.DataFrame({'name': ['john', 'mcclane']}))
        validator.SqlValidator().validate(
            segment='body', field_name='name', arguments=arguments, frames=dataframes
        )
    def test_failure_sql_1to1(self, mocker, dataframes, arguments):
        mocker.patch(
            'transformer.library.aws_service.retrieve_secret',
            return_value={'username': 'test', 'password': 'test'},
        )
        mocker.patch('mysql.connector.connect')
        mocker.patch('pandas.read_sql', return_value=pd.DataFrame({'name': ['tom']}))
        with pytest.raises(ValidationError):
            validator.SqlValidator().validate(
                segment='body', field_name='name', arguments=arguments, frames=dataframes
            )

    def test_failure_sql_many_to_many(self, mocker, dataframes, arguments):
        dataframes['body'] = pd.DataFrame({
            'name': ['john', 'texan'],
            'age': [50,60]
        })
        mocker.patch(
            'transformer.library.aws_service.retrieve_secret',
            return_value={'username': 'test', 'password': 'test'},
        )
        mocker.patch('mysql.connector.connect')
        mocker.patch('pandas.read_sql', return_value=pd.DataFrame({'name': ['john', 'mcclane']}))
        with pytest.raises(ValidationError):
            validator.SqlValidator().validate(
                segment='body', field_name='name', arguments=arguments, frames=dataframes
            )


class TestUniqueValueValidator:
    @pytest.fixture
    def dataframes(self):
        return {'body': pd.DataFrame({'nric': ['S1234567A', 'S1234556B', 'S6543213D', 'S1234567A', 'S1234556C']})}

    def test_unique_error(self, dataframes):
        with pytest.raises(ValidationError):
            validator.UniqueValueValidator().validate('body', 'nric', {}, dataframes)

    def test_unique_success(self, dataframes):
        dataframes['body'] = pd.DataFrame({
            'nric': ['S1234567A', 'S1234556B', 'S6543213D', 'S5231432R', 'S1234556C']
        })
        validator.UniqueValueValidator().validate('body', 'nric', {}, dataframes)