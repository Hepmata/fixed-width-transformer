import os

import pandas as pd
import pytest
import yaml

from tests.helper.fixtures import test_id
from transformer.constraints import constraints
from transformer.library import exceptions
import mysql.connector.connection as connection
from unittest.mock import Mock, MagicMock

from transformer.source import SourceMapperConfig


class TestHashConstraint:
    def test_matching_hash(self, test_id, mocker):
        mocker.patch('transformer.library.common.wait_for_file')
        mocker.patch('transformer.library.aws_service.download_s3_file')
        mocker.patch('transformer.library.aws_service.invoke_lambda', return_value={'statusCode': 200, 'body': 'abc'})
        mocker.patch('transformer.constraints.constraints.open', mocker.mock_open(read_data='abc'))
        config = mocker.MagicMock()
        constraints.HashConstraint().run(
            config,
            arguments={
                'bucket': 'test',
                'file_name': 'test',
                'algorithm': 'test',
                'source_file_path': 'test',
                'max_wait_time': '5',
                'wait_interval': '1'
            }
        )

    def test_matching_hash_with_mount_path(self, test_id, mocker):
        mocker.patch('transformer.library.common.wait_for_file')
        mocker.patch('transformer.library.aws_service.download_s3_file')
        mocker.patch('transformer.library.aws_service.invoke_lambda', return_value={'statusCode': 200, 'body': 'abc'})
        mocker.patch('transformer.constraints.constraints.open', mocker.mock_open(read_data='abc'))
        mocker.patch.dict(os.environ, {'mount_path': '/xyz'})
        config = mocker.MagicMock()
        constraints.HashConstraint().run(
            config,
            arguments={
                'bucket': 'test',
                'file_name': 'test',
                'algorithm': 'test',
                'source_file_path': 'test',
                'max_wait_time': '5',
                'wait_interval': '1'
            }
        )

    def test_not_matching_hash(self, test_id, mocker):
        mocker.patch('transformer.library.common.wait_for_file')
        mocker.patch('transformer.library.aws_service.download_s3_file')
        mocker.patch('transformer.library.aws_service.invoke_lambda', return_value={'statusCode': 200, 'body': 'abc'})
        mocker.patch('transformer.constraints.constraints.open', mocker.mock_open(read_data='def'))
        config = mocker.MagicMock()
        with pytest.raises(exceptions.FailedConstraintException):
            constraints.HashConstraint().run(
                config,
                arguments={
                    'bucket': 'test',
                    'file_name': 'test',
                    'algorithm': 'test',
                    'source_file_path': 'test',
                    'max_wait_time': '5',
                    'wait_interval': '1'
                }
            )

    def test_invalid_algo(self, test_id, mocker):
        mocker.patch('transformer.library.common.wait_for_file')
        mocker.patch('transformer.library.aws_service.download_s3_file')
        mocker.patch('transformer.library.aws_service.invoke_lambda',
                     return_value={'statusCode': 400, 'body': 'invalid algo'})
        mocker.patch('transformer.constraints.constraints.open', mocker.mock_open(read_data='def'))
        config = mocker.MagicMock()
        with pytest.raises(exceptions.FailedConstraintException):
            constraints.HashConstraint().run(
                config,
                arguments={
                    'bucket': 'test',
                    'file_name': 'test',
                    'algorithm': 'test',
                    'source_file_path': 'test',
                    'max_wait_time': '5',
                    'wait_interval': '1'
                }
            )

    def test_missing_file(self, test_id, mocker):
        mocker.patch('transformer.library.common.wait_for_file')
        mocker.patch('transformer.library.aws_service.download_s3_file')
        mocker.patch('transformer.library.aws_service.invoke_lambda',
                     return_value={'statusCode': 500, 'body': 'missing file'})
        mocker.patch('transformer.constraints.constraints.open', mocker.mock_open(read_data='def'))
        config = mocker.MagicMock()
        with pytest.raises(exceptions.FailedConstraintException):
            constraints.HashConstraint().run(
                config,
                arguments={
                    'bucket': 'test',
                    'file_name': 'test',
                    'algorithm': 'test',
                    'source_file_path': 'test',
                    'max_wait_time': '5',
                    'wait_interval': '1'
                }
            )

    def test_missing_arguments(self, test_id, mocker):
        config = mocker.MagicMock()
        with pytest.raises(exceptions.ConstraintMisconfigurationException):
            constraints.HashConstraint().run(
                config,
                arguments={
                    'bucket': 'test',
                    'max_wait_time': '5',
                    'wait_interval': '1'
                }
            )


class TestSqlConstraint:
    def test_valid_with_dataframe(self, test_id, mocker):
        file_text = """FIELD1    FIELD2FIELD3
        UNUSEDDATA1
        UNUSEDDATA2
        """
        mocker.patch('transformer.library.aws_service.retrieve_secret',
                     return_value={'username': 'test', 'password': 'test'})

        mocked = mocker.Mock()
        mocked_cursor = mocker.Mock()
        mocked_cursor.execute.return_value = None
        mocked_cursor.fetchone.return_value = 1
        mocked.cursor.return_value = mocked_cursor
        mocker.patch('mysql.connector.connection.MySQLConnection', return_value=mocked)
        mocker.patch('transformer.constraints.constraints.open', mocker.mock_open(read_data=file_text))
        config = mocker.MagicMock()
        constraints.SqlConstraint().run(
            config,
            arguments={
                'secret_name': 'test/secret',
                'database_host': 'localhost:3306',
                'database_name': 'test',
                'query': 'SELECT name FROM tbl_test WHERE id = {field3}',
                'source_data_segment': 'header',
                'source_colnames': 'field1,field2,field3,filler',
                'source_colspecs': '(0,10),(10,16),(16,26),(26,36)',
                'source_file_path': '/test/file.txt'
            }
        )

    def test_valid_without_dataframe(self, test_id, mocker):
        mocker.patch('transformer.library.aws_service.retrieve_secret',
                     return_value={'username': 'test', 'password': 'test'})

        mocked = mocker.Mock()
        mocked_cursor = mocker.Mock()
        mocked_cursor.execute.return_value = None
        mocked_cursor.fetchone.return_value = 1
        mocked.cursor.return_value = mocked_cursor
        mocker.patch('mysql.connector.connection.MySQLConnection', return_value=mocked)
        config = mocker.MagicMock()
        constraints.SqlConstraint().run(
            config,
            arguments={
                'secret_name': 'test/secret',
                'database_host': 'localhost:3306',
                'database_name': 'test',
                'query': 'SELECT name FROM tbl_test WHERE id = {file_name}',
                'source_file_path': '/test/file.txt'
            }
        )

    def test_valid_without_reference(self, test_id, mocker):
        mocker.patch('transformer.library.aws_service.retrieve_secret',
                     return_value={'username': 'test', 'password': 'test'})

        mocked = mocker.Mock()
        mocked_cursor = mocker.Mock()
        mocked_cursor.execute.return_value = None
        mocked_cursor.fetchone.return_value = 1
        mocked.cursor.return_value = mocked_cursor
        mocker.patch('mysql.connector.connection.MySQLConnection', return_value=mocked)
        config = mocker.MagicMock()
        constraints.SqlConstraint().run(
            config,
            arguments={
                'secret_name': 'test/secret',
                'database_host': 'localhost:3306',
                'database_name': 'test',
                'query': 'SELECT name FROM tbl_test WHERE id = 123',
                'source_file_path': '/test/file.txt'
            }
        )

    def test_valid_colspec_space(self, test_id, mocker):
        file_text = """FIELD1    FIELD2FIELD3
        UNUSEDDATA1
        UNUSEDDATA2
        """
        mocker.patch('transformer.library.aws_service.retrieve_secret',
                     return_value={'username': 'test', 'password': 'test'})
        mocked = mocker.Mock()
        mocked_cursor = mocker.Mock()
        mocked_cursor.execute.return_value = None
        mocked_cursor.fetchone.return_value = 500
        mocked.cursor.return_value = mocked_cursor
        mocker.patch('mysql.connector.connection.MySQLConnection', return_value=mocked)
        mocker.patch('transformer.constraints.constraints.open', mocker.mock_open(read_data=file_text))
        config = mocker.MagicMock()
        constraints.SqlConstraint().run(
            config,
            arguments={
                'secret_name': 'test/secret',
                'database_host': 'localhost:3306',
                'database_name': 'test',
                'query': 'SELECT name FROM tbl_test WHERE id = {field3}',
                'source_data_segment': 'header',
                'source_colnames': 'field1,field2,field3,filler',
                'source_colspecs': '(0, 10), (10,        16),(16, 26),(26, 36)',
                'source_file_path': '/test/file.txt'
            }
        )

    def test_invalid_no_rows_found(self, test_id, mocker):
        file_text = """FIELD1    FIELD2FIELD3
        UNUSEDDATA1
        UNUSEDDATA2
        """
        mocker.patch('transformer.library.aws_service.retrieve_secret',
                     return_value={'username': 'test', 'password': 'test'})
        mocked = mocker.Mock()
        mocked_cursor = mocker.Mock()
        mocked_cursor.execute.return_value = None
        mocked_cursor.fetchone.return_value = None
        mocked.cursor.return_value = mocked_cursor
        mocker.patch('mysql.connector.connection.MySQLConnection', return_value=mocked)
        m = mocker.patch.object(connection.MySQLConnection.cursor, 'fetchone')
        m.return_value = None
        mocker.patch('transformer.constraints.constraints.open',
                     mocker.mock_open(read_data=file_text))
        config = mocker.MagicMock()
        with pytest.raises(exceptions.FailedConstraintException):
            constraints.SqlConstraint().run(
                config,
                arguments={
                    'secret_name': 'test/secret',
                    'database_host': 'localhost:3306',
                    'database_name': 'test',
                    'query': 'SELECT name FROM tbl_test WHERE id = {field3}',
                    'source_data_segment': 'header',
                    'source_colnames': 'field1,field2,field3,filler',
                    'source_colspecs': '(0,10),(10,16),(16,26),(26,36)',
                    'source_file_path': '/test/file.txt'
                }
            )

    def test_invalid_missing_arguments(self, test_id, mocker):
        with pytest.raises(exceptions.ConstraintMisconfigurationException) as e:
            config = mocker.MagicMock()
            constraints.SqlConstraint().run(
                config,
                arguments={
                    'source_data_segment': 'header',
                    'source_colnames': 'field1,field2,field3,filler',
                    'source_colspecs': '(0,10),(10,16),(16,26),(26,36)',
                    'source_file_path': '/test/file.txt'
                }
            )
            missing_fields = ['secret_name', 'database_host', 'database_name', 'query']
            assert e.value == f'Required arguments {missing_fields} are missing for Constraint {constraints.SqlConstraint.__name__}'


class TestS3FileRefConstraint:

    @pytest.fixture
    def source_df(self):
        return {
            'header': pd.DataFrame({
                'recordType': ['1'],
                'reportGenerationDate': ['20200101']
            }),
            'body': pd.DataFrame({
                'nric': ['S0000001A', 'S0000002B', 'S0000003C'],
                'amount': ['100', '200', '100'],
                'name': ['Tom1', 'Tom2', 'Tom3']
            }),
            'trailer': pd.DataFrame({
                'recordType': ['1']
            })
        }

    @pytest.fixture
    def ref_df(self):
        return {
            'header': pd.DataFrame({
                'recordType': ['1'],
                'reportGenerationDate': ['20200101']
            }),
            'body': pd.DataFrame({
                'nric': ['S0000002B', 'S0000003C', 'S0000001A'],
                'amount': ['200', '100', '100'],
                'name': ['Tom2', 'Tom3', 'Tom1']
            }),
            'trailer': pd.DataFrame({
                'recordType': ['1']
            })
        }

    @pytest.fixture
    def config_text(self):
        return """
        header:
          formatter: CSVHeaderFormatter
          format:
            - name: recordType
            - name: reportGenerationDate
        body:
          formatter: CSVBodyFormatter
          format:
            - name: nric
            - name: amount
        trailer:
          formatter: CSVTrailerFormatter
          format:
            - name: recordType
        """


    def test_compare_valid_mixed(self, test_id, config_text, source_df, ref_df, mocker):
        constraint = constraints.S3FileRefConstraint()
        constraint.ref_dataframes = ref_df
        constraint.source_dataframes = source_df
        constraint.compare(
            arguments={
                'file_name': '/egress/test.txt',
                'bucket': 'test-bucket',
                'format': 'test.txt',
                'config': [{
                    'source_segment': 'body',
                    'ref_segment': 'body',
                    'source_aggregate': 'nric',
                    'ref_aggregate': 'nric',
                    'fields': [{
                        'source': 'amount',
                        'ref': 'amount'
                    }, {
                        'source': 'name',
                        'ref': 'name'
                    }]
                }, {
                    'source_segment': 'header',
                    'ref_segment': 'header',
                    'fields': [{
                        'source': 'recordType',
                        'ref': 'recordType'
                    }]
                }]
            }
        )

    def test_compare_valid_with_aggregate(self, test_id, config_text, source_df, ref_df, mocker):
        constraint = constraints.S3FileRefConstraint()
        constraint.ref_dataframes = ref_df
        constraint.source_dataframes = source_df
        constraint.compare(
            arguments={
                'file_name': '/egress/test.txt',
                'bucket': 'test-bucket',
                'format': 'test.txt',
                'config': [{
                    'source_segment': 'body',
                    'ref_segment': 'body',
                    'source_aggregate': 'nric',
                    'ref_aggregate': 'nric',
                    'fields': [{
                        'source': 'amount',
                        'ref': 'amount'
                    }, {
                        'source': 'name',
                        'ref': 'name'
                    }]
                }]
            }
        )

    def test_compare_valid_without_aggregate(self, test_id, config_text, source_df, ref_df, mocker):
        constraint = constraints.S3FileRefConstraint()
        constraint.ref_dataframes = ref_df
        constraint.source_dataframes = source_df
        constraint.compare(arguments={
            'file_name': '/egress/test.txt',
            'bucket': 'test-bucket',
            'format': 'test.txt',
            'config': [{
                'source_segment': 'header',
                'ref_segment': 'header',
                'fields': [{
                    'source': 'recordType',
                    'ref': 'recordType'
                }]
            }]
        })

    def test_compare_invalid_wrong_size(self, test_id, config_text, source_df, ref_df, mocker):
        constraint = constraints.S3FileRefConstraint()
        constraint.ref_dataframes = ref_df
        source_df['body'] = pd.DataFrame({
            'nric': ['S0000002B', 'S0000003C'],
            'amount': ['200', '100'],
            'name': ['Tom2', 'Tom3']
        })
        constraint.source_dataframes = source_df
        with pytest.raises(exceptions.FailedConstraintsException):
            constraint.compare(arguments={
                'file_name': '/egress/test.txt',
                'bucket': 'test-bucket',
                'format': 'test.txt',
                'config': [{
                    'source_segment': 'body',
                    'ref_segment': 'body',
                    'fields': [{
                        'source': 'nric',
                        'ref': 'nric'
                    }]
                }]
            })

    def test_compare_invalid_data_mismatch(self, test_id, config_text, source_df, ref_df, mocker):
        constraint = constraints.S3FileRefConstraint()
        constraint.ref_dataframes = ref_df
        source_df['body'] = pd.DataFrame({
            'nric': ['S0000002B', 'S0000003C', 'S0000001A'],
            'amount': ['600', '100', '100'],
            'name': ['Tom2', 'Tom3', 'Tom1']
        })
        constraint.source_dataframes = source_df
        with pytest.raises(exceptions.FailedConstraintsException):
            constraint.compare(arguments={
                'file_name': '/egress/test.txt',
                'bucket': 'test-bucket',
                'format': 'test.txt',
                'config': [{
                    'source_segment': 'body',
                    'ref_segment': 'body',
                    'source_aggregate': 'nric',
                    'ref_aggregate': 'nric',
                    'fields': [{
                        'source': 'amount',
                        'ref': 'amount'
                    }]
                }]
            })

    def test_compare_missing_aggregate(self, test_id, config_text, source_df, ref_df, mocker):
        constraint = constraints.S3FileRefConstraint()
        constraint.ref_dataframes = ref_df
        source_df['body'] = pd.DataFrame({
            'nric': ['S0000002B', 'S0000003C', 'S0000001A'],
            'amount': ['600', '100', '100'],
            'name': ['Tom2', 'Tom3', 'Tom1']
        })
        constraint.source_dataframes = source_df
        with pytest.raises(exceptions.ConstraintMisconfigurationException):
            constraint.compare(arguments={
                'file_name': '/egress/test.txt',
                'bucket': 'test-bucket',
                'format': 'test.txt',
                'config': [{
                    'source_segment': 'body',
                    'ref_segment': 'body',
                    'source_aggregate': 'nric',
                    'fields': [{
                        'source': 'amount',
                        'ref': 'amount'
                    }]
                }]
            })

    def test_map_external_config_valid(self, config_text, mocker):
        mocker.patch('transformer.constraints.constraints.open',
                     mocker.mock_open(read_data=config_text))
        constraint = constraints.S3FileRefConstraint()
        result = constraint.map_external_config('test.txt')
        expected = SourceMapperConfig({'source': yaml.safe_load(config_text)}, '')
        assert type(result) == constraints.SourceMapperConfig
        assert expected == result

    def test_map_external_dataframes_valid(self, mocker):
        mocker.patch('transformer.library.aws_service.download_s3_file', return_value='')
        source_mapper = Mock()
        expected = {'header': pd.DataFrame()}
        source_mapper.run.return_value = expected
        mocker.patch('transformer.constraints.constraints.SourceMapper', return_value=source_mapper)
        mocked_config = Mock()
        mocked_config.file_name = ''
        constraint = constraints.S3FileRefConstraint()
        constraint.external_format_config = mocked_config
        result = constraint.map_external_dataframes('', '')
        assert result == expected

    def test_run_valid(self, source_df, config_text, mocker):
        expected_config = SourceMapperConfig({'source': yaml.safe_load(config_text)}, '')
        source_mapper = Mock()
        source_mapper.run.return_value = source_df
        mocker.patch('transformer.library.aws_service.download_s3_file', return_value='')
        mocker.patch('transformer.constraints.constraints.SourceMapper', return_value=source_mapper)
        mocker.patch('transformer.constraints.constraints.open',
                     mocker.mock_open(read_data=config_text))
        constraint = constraints.S3FileRefConstraint()
        constraint.run(expected_config, arguments={
                'file_name': '/egress/test.txt',
                'bucket': 'test-bucket',
                'format': 'test.txt',
                'source_file_path': 'test',
                'config': [{
                    'source_segment': 'header',
                    'ref_segment': 'header',
                    'fields': [{
                        'source': 'recordType',
                        'ref': 'recordType'
                    }]
                }]
            })

    def test_run_invalid(self, source_df, config_text, mocker):
        expected_config = SourceMapperConfig({'source': yaml.safe_load(config_text)}, '')
        source_mapper = Mock()
        source_mapper.run.return_value = source_df
        mocker.patch('transformer.library.aws_service.download_s3_file', return_value='')
        mocker.patch('transformer.constraints.constraints.SourceMapper', return_value=source_mapper)
        mocker.patch('transformer.constraints.constraints.open',
                     mocker.mock_open(read_data=config_text))
        constraint = constraints.S3FileRefConstraint()
        with pytest.raises(exceptions.FailedConstraintsException):
            constraint.run(expected_config, arguments={
                    'file_name': '/egress/test.txt',
                    'bucket': 'test-bucket',
                    'format': 'test.txt',
                    'source_file_path': 'test',
                    'config': [{
                        'source_segment': 'body',
                        'ref_segment': 'body',
                        'source_aggregate': 'nric',
                        'ref_aggregate': 'nric',
                        'fields': [{
                            'source': 'name',
                            'ref': 'amount'
                        }]
                    }]
                })
