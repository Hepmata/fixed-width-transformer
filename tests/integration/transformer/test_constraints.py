import datetime
import multiprocessing
import os
import uuid
from os import listdir, remove
from os.path import isfile, join
from time import sleep
from transformer.source import SourceMapperConfig
import pytest
import mysql.connector.connection as connection
import yaml

from tests.helper.fixtures import test_id
from tests.helper import aws_helper, file_helper
from transformer.constraints import constraints
from transformer.library import exceptions
from unittest.mock import patch
project_root_path = os.path.abspath(os.path.dirname(__file__))


def wait_and_upload(fc, fc_name, b):
    sleep(2)

    file_helper.create_file(fc, fc_name)
    aws_helper.upload_s3_file(file_name=fc_name, s3_file_key=fc_name, bucket=b)


def teardown_module():
    files = [f for f in listdir('./') if isfile(join('./', f))]
    for file in files:
        if file.endswith('.txt') or file.endswith('.yml'):
            remove(file)


@patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
class TestHashConstraint:

    @pytest.fixture
    def test_id(self):
        eid = uuid.uuid4().__str__()
        return eid.replace('-', '')

    @pytest.fixture
    def file_content(self):
        return {
            'header': [{
                'length': 30,
                'value': 'hello'
            }],
            'body': [{
                'length': 30,
                'value': 'hello x2'
            }],
            'footer': [{
                'length': 30,
                'value': 'hello x3'
            }]
        }

    def test_valid_txt(self, file_content, test_id, mocker):
        file_name = f'{test_id}.txt'
        file_hash_name = f'{test_id}-hash.txt'
        bucket = f'{test_id}-bucket'
        # Generate and upload data file
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.upload_s3_file(file_name=file_name, s3_file_key=file_name, bucket=bucket)
        # Generate and upload hash file
        hash_result = file_helper.generate_hash_from_file(file_name)
        file_helper.create_file(hash_result, file_hash_name)
        aws_helper.upload_s3_file(file_name=file_hash_name, s3_file_key=file_hash_name, bucket=bucket)
        # Mock Hash lambda call to prevent hitting QA
        mocker.patch('transformer.library.aws_service.invoke_lambda',
                     return_value={'statusCode': 200, 'body': hash_result})
        constraints.HashConstraint().run(
            mocker.MagicMock(),
            arguments={
                'bucket': bucket,
                'file_name': file_hash_name,
                'algorithm': 'SHA-256',
                'source_file_path': file_name,
                'max_wait_time': '5',
                'wait_interval': '1'
            }
        )

    def test_valid_txt_with_whitespaces(self, file_content, test_id, mocker):
        file_name = f'{test_id}.txt'
        file_hash_name = f'{test_id}-hash.txt'
        bucket = f'{test_id}-bucket'
        # Generate and upload data file
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.upload_s3_file(file_name=file_name, s3_file_key=file_name, bucket=bucket)
        # Generate and upload hash file
        hash_result = file_helper.generate_hash_from_file(file_name)
        file_helper.create_file(hash_result + '            ', file_hash_name)
        aws_helper.upload_s3_file(file_name=file_hash_name, s3_file_key=file_hash_name, bucket=bucket)
        # Mock Hash lambda call to prevent hitting QA
        mocker.patch('transformer.library.aws_service.invoke_lambda',
                     return_value={'statusCode': 200, 'body': hash_result})
        constraints.HashConstraint().run(
            mocker.MagicMock(),
            arguments={
                'bucket': bucket,
                'file_name': file_hash_name,
                'algorithm': 'SHA-256',
                'source_file_path': file_name,
                'max_wait_time': '5',
                'wait_interval': '1'
            }
        )

    def test_unmatching_hashes(self, file_content, test_id, mocker):
        file_name = f'{test_id}.txt'
        file_hash_name = f'{test_id}-hash.txt'
        bucket = f'{test_id}-bucket'
        # Generate and upload data file
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.upload_s3_file(file_name=file_name, s3_file_key=file_name, bucket=bucket)
        # Generate and upload hash file
        hash_result = file_helper.generate_hash_from_file(file_name)
        file_helper.create_file(hash_result, file_hash_name)
        aws_helper.upload_s3_file(file_name=file_hash_name, s3_file_key=file_hash_name, bucket=bucket)

        # Mock Hash lambda call to prevent hitting QA
        mocker.patch('transformer.library.aws_service.invoke_lambda',
                     return_value={'statusCode': 200, 'body': 'abc'})
        with pytest.raises(exceptions.FailedConstraintException) as e:
            constraints.HashConstraint().run(
                mocker.MagicMock(),
                arguments={
                    'bucket': bucket,
                    'file_name': file_hash_name,
                    'algorithm': 'SHA-256',
                    'source_file_path': file_name,
                    'max_wait_time': '5',
                    'wait_interval': '1'
                }
            )
            assert str(
                e.value) == f'Failed HashConstraint with unmatching hashes. Hash File: {hash_result}, Hash Content: abc'

    def test_invalid_algorithm(self, file_content, test_id, mocker):
        file_name = f'{test_id}.txt'
        file_hash_name = f'{test_id}-hash.txt'
        bucket = f'{test_id}-bucket'
        # Generate and upload data file
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.upload_s3_file(file_name=file_name, s3_file_key=file_name, bucket=bucket)
        # Generate and upload hash file
        hash_result = file_helper.generate_hash_from_file(file_name)
        file_helper.create_file(hash_result, file_hash_name)
        aws_helper.upload_s3_file(file_name=file_hash_name, s3_file_key=file_hash_name, bucket=bucket)

        # Mock Hash lambda call to prevent hitting QA
        mocker.patch('transformer.library.aws_service.invoke_lambda',
                     return_value={'statusCode': 400,
                                   'body': 'Provided Algorithm of SHA-000 is invalid or unsupported.'})
        with pytest.raises(exceptions.FailedConstraintException) as e:
            constraints.HashConstraint().run(
                mocker.MagicMock(),
                arguments={
                    'bucket': bucket,
                    'file_name': file_hash_name,
                    'algorithm': 'SHA-000',
                    'source_file_path': file_name,
                    'max_wait_time': '5',
                    'wait_interval': '1'
                }
            )
            assert str(
                e.value) == f'Failed to get hash from FileTransformerHash. StatusCode is 400 with message Provided Algorithm of SHA-000 is invalid or unsupported.'

    def test_wait_2_seconds(self, file_content, test_id, mocker):
        file_name = f'{test_id}.txt'
        file_hash_name = f'{test_id}-hash.txt'
        bucket = f'{test_id}-bucket'
        # Generate and upload data file
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.upload_s3_file(file_name=file_name, s3_file_key=file_name, bucket=bucket)
        # Generate and upload hash file
        hash_result = file_helper.generate_hash_from_file(file_name)

        # Mock Hash lambda call to prevent hitting QA
        mocker.patch('transformer.library.aws_service.invoke_lambda',
                     return_value={'statusCode': 200, 'body': hash_result})
        p = multiprocessing.Process(target=wait_and_upload, args=(hash_result, file_hash_name, bucket,))
        p.start()
        constraints.HashConstraint().run(
            mocker.MagicMock(),
            arguments={
                'bucket': bucket,
                'file_name': file_hash_name,
                'algorithm': 'SHA-000',
                'source_file_path': file_name,
                'max_wait_time': '5',
                'wait_interval': '1'
            }
        )


class TestSqlConstraint:
    @pytest.fixture
    def file_content(self):
        return {
            'header': [{
                'length': 1,
                'value': 'Y'
            }, {
                'length': 10,
                'value': '20200101'
            }, {
                'length': 20,
                'value': 'NICEFILE'
            }, {
                'length': 50,
                'value': ' '
            }],
            'body': [{
                'length': 50,
                'value': 'UNUSEDCONTENT'
            }],
            'footer': [{
                'length': 5,
                'value': '1'
            }]
        }

    def test_valid_without_reference(self, test_id, file_content, mocker):
        mocker.patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
        current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_name = f'{test_id}.txt'
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.create_secret(secret_name='test/mysql',
                                 content={'username': 'test_user', 'password': 'integration-test'},
                                 request_token=test_id
                                 )
        print('hi')
        sql = connection.MySQLConnection(
            user='test_user',
            password='integration-test',
            host='localhost',
            port=3307,
            database='test_db'
        )

        cursor = sql.cursor(buffered=True)
        cursor.execute(f"""
        CREATE TABLE tbl_{test_id} (
            id int auto_increment primary key,
            updated_ts TIMESTAMP default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            updated_date DATETIME default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            file_name varchar(255)
        );""")
        cursor.execute(f"""
        INSERT INTO tbl_{test_id} (file_name, updated_ts, updated_date)
        VALUES (%s,%s,%s)""", params=(file_name, current_timestamp, current_timestamp))
        sql.commit()
        cursor.close()
        sql.close()

        constraints.SqlConstraint().run(
            mocker.MagicMock(),
            arguments={
                'secret_name': 'test/mysql',
                'database_host': 'localhost',
                'database_name': 'test_db',
                'database_port': '3307',
                'query': f'SELECT * FROM tbl_{test_id} WHERE id = 1',
                'source_file_path': f'{project_root_path}/{test_id}.txt'
            }
        )

    def test_valid_without_dataframe(self, test_id, file_content, mocker):
        mocker.patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
        current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_name = f'{test_id}.txt'
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.create_secret(secret_name='test/mysql',
                                 content={'username': 'test_user', 'password': 'integration-test'},
                                 request_token=test_id
                                 )
        sql = connection.MySQLConnection(
            user='test_user',
            password='integration-test',
            host='localhost',
            port=3307,
            database='test_db'
        )
        cursor = sql.cursor(buffered=True)
        cursor.execute(f"""
        CREATE TABLE tbl_{test_id} (
            id int auto_increment primary key,
            updated_ts TIMESTAMP default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            updated_date DATETIME default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            file_name varchar(255)
        );""")
        cursor.execute(f"""
        INSERT INTO tbl_{test_id} (file_name, updated_ts, updated_date)
        VALUES (%s,%s,%s)""", params=(file_name, current_timestamp, current_timestamp))
        sql.commit()
        cursor.close()
        sql.close()
        query_pt1 = f'SELECT * FROM tbl_{test_id}'
        constraints.SqlConstraint().run(
            mocker.MagicMock(),
            arguments={
                'secret_name': 'test/mysql',
                'database_host': 'localhost',
                'database_name': 'test_db',
                'database_port': '3307',
                'query': query_pt1 + " WHERE file_name = '{file_name}'",
                'source_file_path': f'{project_root_path}/{test_id}.txt'
            }
        )

    def test_valid_with_dataframe_tslike(self, test_id, file_content, mocker):
        mocker.patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
        time_string = '20200101'
        current_timestamp = datetime.datetime.strptime(time_string, '%Y%m%d')
        file_name = f'{test_id}.txt'
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.create_secret(secret_name='test/mysql',
                                 content={'username': 'test_user', 'password': 'integration-test'},
                                 request_token=test_id
                                 )
        sql = connection.MySQLConnection(
            user='test_user',
            password='integration-test',
            host='localhost',
            port=3307,
            database='test_db'
        )
        cursor = sql.cursor(buffered=True)
        cursor.execute(f"""
        CREATE TABLE tbl_{test_id} (
            id int auto_increment primary key,
            updated_ts TIMESTAMP default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            updated_date DATETIME default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            file_name varchar(255)
        );""")
        cursor.execute(f"""
        INSERT INTO tbl_{test_id} (file_name, updated_ts, updated_date)
        VALUES (%s,%s,%s)""", params=(file_name, current_timestamp, current_timestamp))
        sql.commit()
        cursor.close()
        sql.close()
        query_pt1 = f'SELECT * FROM tbl_{test_id}'
        constraints.SqlConstraint().run(
            mocker.MagicMock(),
            arguments={
                'secret_name': 'test/mysql',
                'database_host': 'localhost',
                'database_name': 'test_db',
                'database_port': '3307',
                'query': query_pt1 + " WHERE file_name = '{file_name}' AND updated_date LIKE '{tx_date}%'",
                'source_file_path': f'{project_root_path}/{test_id}.txt',
                'source_data_segment': 'header',
                'source_colnames': 'file_type,tx_date,others',
                'source_colspecs': '(0,1),(1,10),(10,30)',
                'source_date_format': '%Y%m%d'
            }
        )

    def test_valid_with_dataframe_tsequals(self, test_id, file_content, mocker):
        mocker.patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
        time_string = '20200101'
        current_timestamp = datetime.datetime.strptime(time_string, '%Y%m%d')
        file_name = f'{test_id}.txt'
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.create_secret(secret_name='test/mysql',
                                 content={'username': 'test_user', 'password': 'integration-test'},
                                 request_token=test_id
                                 )
        sql = connection.MySQLConnection(
            user='test_user',
            password='integration-test',
            host='localhost',
            port=3307,
            database='test_db'
        )
        cursor = sql.cursor(buffered=True)
        cursor.execute(f"""
        CREATE TABLE tbl_{test_id} (
            id int auto_increment primary key,
            updated_ts TIMESTAMP default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            updated_date DATETIME default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            file_name varchar(255)
        );""")
        cursor.execute(f"""
        INSERT INTO tbl_{test_id} (file_name, updated_ts, updated_date)
        VALUES (%s,%s,%s)""", params=(file_name, current_timestamp, current_timestamp))
        sql.commit()
        cursor.close()
        sql.close()
        query_pt1 = f'SELECT * FROM tbl_{test_id}'
        constraints.SqlConstraint().run(
            mocker.MagicMock(),
            arguments={
                'secret_name': 'test/mysql',
                'database_host': 'localhost',
                'database_name': 'test_db',
                'database_port': '3307',
                'query': query_pt1 + " WHERE file_name = '{file_name}' AND updated_date >= '{tx_date}'",
                'source_file_path': f'{project_root_path}/{test_id}.txt',
                'source_data_segment': 'header',
                'source_colnames': 'file_type,tx_date,others',
                'source_colspecs': '(0,1),(1,10),(10,30)',
                'source_date_format': '%Y%m%d'
            }
        )

    def test_invalid_without_dataframe(self, test_id, file_content, mocker):
        mocker.patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
        current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_name = f'{test_id}.txt'
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.create_secret(secret_name='test/mysql',
                                 content={'username': 'test_user', 'password': 'integration-test'},
                                 request_token=test_id
                                 )
        sql = connection.MySQLConnection(
            user='test_user',
            password='integration-test',
            host='localhost',
            port=3307,
            database='test_db'
        )
        cursor = sql.cursor(buffered=True)
        cursor.execute(f"""
        CREATE TABLE tbl_{test_id} (
            id int auto_increment primary key,
            updated_ts TIMESTAMP default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            updated_date DATETIME default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            file_name varchar(255)
        );""")
        cursor.execute(f"""
        INSERT INTO tbl_{test_id} (file_name, updated_ts, updated_date)
        VALUES (%s,%s,%s)""", params=('rubbish', current_timestamp, current_timestamp))
        sql.commit()
        cursor.close()
        sql.close()
        query_pt1 = f'SELECT * FROM tbl_{test_id}'
        with pytest.raises(exceptions.FailedConstraintException):
            constraints.SqlConstraint().run(
                mocker.MagicMock(),
                arguments={
                    'secret_name': 'test/mysql',
                    'database_host': 'localhost',
                    'database_name': 'test_db',
                    'database_port': '3307',
                    'query': query_pt1 + " WHERE file_name = '{file_name}'",
                    'source_file_path': f'{project_root_path}/{test_id}.txt'
                }
            )

    def test_invalid_without_reference(self, test_id, file_content, mocker):
        mocker.patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
        current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        file_name = f'{test_id}.txt'
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.create_secret(secret_name='test/mysql',
                                 content={'username': 'test_user', 'password': 'integration-test'},
                                 request_token=test_id
                                 )
        sql = connection.MySQLConnection(
            user='test_user',
            password='integration-test',
            host='localhost',
            port=3307,
            database='test_db'
        )
        cursor = sql.cursor(buffered=True)
        cursor.execute(f"""
        CREATE TABLE tbl_{test_id} (
            id int auto_increment primary key,
            updated_ts TIMESTAMP default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            updated_date DATETIME default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            file_name varchar(255)
        );""")
        cursor.execute(f"""
        INSERT INTO tbl_{test_id} (file_name, updated_ts, updated_date)
        VALUES (%s,%s,%s)""", params=(file_name, current_timestamp, current_timestamp))
        sql.commit()
        cursor.close()
        sql.close()
        with pytest.raises(exceptions.FailedConstraintException):
            constraints.SqlConstraint().run(
                mocker.MagicMock(),
                arguments={
                    'secret_name': 'test/mysql',
                    'database_host': 'localhost',
                    'database_name': 'test_db',
                    'database_port': '3307',
                    'query': f'SELECT * FROM tbl_{test_id} WHERE id = 2',
                    'source_file_path': f'{project_root_path}/{test_id}.txt'
                }
            )

    def test_invalid_with_dataframe_tsequals(self, test_id, file_content, mocker):
        mocker.patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
        time_string = '20190101'
        current_timestamp = datetime.datetime.strptime(time_string, '%Y%m%d')
        file_name = f'{test_id}.txt'
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.create_secret(secret_name='test/mysql',
                                 content={'username': 'test_user', 'password': 'integration-test'},
                                 request_token=test_id
                                 )
        sql = connection.MySQLConnection(
            user='test_user',
            password='integration-test',
            host='localhost',
            port=3307,
            database='test_db'
        )
        cursor = sql.cursor(buffered=True)
        cursor.execute(f"""
        CREATE TABLE tbl_{test_id} (
            id int auto_increment primary key,
            updated_ts TIMESTAMP default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            updated_date DATETIME default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            file_name varchar(255)
        );""")
        cursor.execute(f"""
        INSERT INTO tbl_{test_id} (file_name, updated_ts, updated_date)
        VALUES (%s,%s,%s)""", params=(file_name, current_timestamp, current_timestamp))
        sql.commit()
        cursor.close()
        sql.close()
        query_pt1 = f'SELECT * FROM tbl_{test_id}'

        with pytest.raises(exceptions.FailedConstraintException):
            constraints.SqlConstraint().run(
                mocker.MagicMock(),
                arguments={
                    'secret_name': 'test/mysql',
                    'database_host': 'localhost',
                    'database_name': 'test_db',
                    'database_port': '3307',
                    'query': query_pt1 + " WHERE file_name = '{file_name}' AND updated_date >= '{tx_date}'",
                    'source_file_path': f'{project_root_path}/{test_id}.txt',
                    'source_data_segment': 'header',
                    'source_colnames': 'file_type,tx_date,others',
                    'source_colspecs': '(0,1),(1,10),(10,30)',
                    'source_date_format': '%Y%m%d'
                }
            )

    def test_invalid_with_dataframe_tslike(self, test_id, file_content, mocker):
        mocker.patch('transformer.library.aws_service.common.build_aws_endpoint_url', return_value=os.environ['AWS_ENDPOINT_URL'])
        time_string = '20190101'
        current_timestamp = datetime.datetime.strptime(time_string, '%Y%m%d')
        file_name = f'{test_id}.txt'
        file_helper.create_fixed_width_file(content=file_content, file_name=file_name)
        aws_helper.create_secret(secret_name='test/mysql',
                                 content={'username': 'test_user', 'password': 'integration-test'},
                                 request_token=test_id
                                 )
        sql = connection.MySQLConnection(
            user='test_user',
            password='integration-test',
            host='localhost',
            port=3307,
            database='test_db'
        )
        cursor = sql.cursor(buffered=True)
        cursor.execute(f"""
        CREATE TABLE tbl_{test_id} (
            id int auto_increment primary key,
            updated_ts TIMESTAMP default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            updated_date DATETIME default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            file_name varchar(255)
        );""")
        cursor.execute(f"""
        INSERT INTO tbl_{test_id} (file_name, updated_ts, updated_date)
        VALUES (%s,%s,%s)""", params=(file_name, current_timestamp, current_timestamp))
        sql.commit()
        cursor.close()
        sql.close()
        query_pt1 = f'SELECT * FROM tbl_{test_id}'

        with pytest.raises(exceptions.FailedConstraintException):
            constraints.SqlConstraint().run(
                mocker.MagicMock(),
                arguments={
                    'secret_name': 'test/mysql',
                    'database_host': 'localhost',
                    'database_name': 'test_db',
                    'database_port': '3307',
                    'query': query_pt1 + " WHERE file_name = '{file_name}' AND updated_date LIKE '{tx_date}%'",
                    'source_file_path': f'{project_root_path}/{test_id}.txt',
                    'source_data_segment': 'header',
                    'source_colnames': 'file_type,tx_date,others',
                    'source_colspecs': '(0,1),(1,10),(10,30)',
                    'source_date_format': '%Y%m%d'
                }
            )


class TestS3RefConstraint:

    def test_valid_without_aggregate(self, test_id, mocker):
        # Create source data file

        source_file_content = {
            'header': [
                {'length': 1, 'value': 'Y'},
                {'length': 8, 'value': '20200101'},
            ],
            'body': [
                [
                    {'length': 9, 'value': 'S2995883A'},
                    {'length': 50, 'value': 'John Hopkins'},
                    {'length': 10, 'value': '1000'},
                    {'length': 5, 'value': '00001'}
                ],
                [
                    {'length': 9, 'value': 'S5726556F'},
                    {'length': 50, 'value': 'Robert Goddard'},
                    {'length': 10, 'value': '1443'},
                    {'length': 5, 'value': '00002'}
                ],
                [
                    {'length': 9, 'value': 'S0981459J'},
                    {'length': 50, 'value': 'Katherine Johnson'},
                    {'length': 10, 'value': '9882'},
                    {'length': 5, 'value': '00003'}
                ]
            ],
            'footer': [{'length': 20, 'value': '00000000000010001001'}]
        }
        file_helper.create_multiline_fixed_width_file(source_file_content, f'{test_id}-source.txt')

        # Create reference data file
        ref_file_content = {
            'header': [
                {'length': 1, 'value': 'Y'},
                {'length': 8, 'value': '20200101'},
            ],
            'body': [
                [
                    {'length': 9, 'value': 'S0981459J'},
                    {'length': 50, 'value': 'Katherine Johnson'},
                    {'length': 10, 'value': '9882'},
                    {'length': 5, 'value': '00003'}
                ],
                [
                    {'length': 9, 'value': 'S2995883A'},
                    {'length': 50, 'value': 'John Hopkins'},
                    {'length': 10, 'value': '1000'},
                    {'length': 5, 'value': '00001'}
                ],
                [
                    {'length': 9, 'value': 'S5726556F'},
                    {'length': 50, 'value': 'Robert Goddard'},
                    {'length': 10, 'value': '1443'},
                    {'length': 5, 'value': '00002'}
                ]
            ],
            'footer': [{'length': 20, 'value': '000000000000100010'}]
        }
        ref_file_name = f'{test_id}-ref.txt'
        file_helper.create_multiline_fixed_width_file(ref_file_content, ref_file_name)
        bucket_name = 'integration-test'
        aws_helper.upload_s3_file(file_name=f'{test_id}-ref.txt', bucket=bucket_name, s3_file_key=ref_file_name)
        # Create source config
        config_text = """
        header:
          formatter: HeaderSourceFormatter
          format:
            - name: recordType
              spec: 0,1
            - name: reportGenerationDate
              spec: 1,9
        body:
          formatter: BodySourceFormatter
          format:
            - name: nric
              spec: 0,9
            - name: name
              spec: 9,59
            - name: amount
              spec: 59,69
            - name: internal_id
              spec: 69,74
        footer:
          formatter: FooterSourceFormatter
          format:
            - name: recordType
              spec: 0,20
        """
        config_dict = yaml.safe_load(config_text)
        source_mapper_cfg = SourceMapperConfig({'source': config_dict}, file_name=f'{test_id}-source.txt')
        # Create ref config
        file_helper.create_file(config_text, f'{test_id}-ref-cfg.yml')
        constraint = constraints.S3FileRefConstraint()

        mocker.patch.dict(os.environ, {'mount_path': project_root_path})
        constraint.run(source_mapper_cfg, arguments={
                'file_name': ref_file_name,
                'bucket': bucket_name,
                'format': f'{test_id}-ref-cfg.yml',
                'source_file_path': '/test/file.txt',
                'config': [{
                    'source_segment': 'header',
                    'ref_segment': 'header',
                    'fields': [{
                        'source': 'recordType',
                        'ref': 'recordType'
                    }]
                }]
            })

    def test_valid_with_aggregate(self, test_id, mocker):
        # Create source data file

        source_file_content = {
            'header': [
                {'length': 1, 'value': 'Y'},
                {'length': 8, 'value': '20200101'},
            ],
            'body': [
                [
                    {'length': 9, 'value': 'S2995883A'},
                    {'length': 50, 'value': 'John Hopkins'},
                    {'length': 10, 'value': '1000'},
                    {'length': 5, 'value': '00001'}
                ],
                [
                    {'length': 9, 'value': 'S5726556F'},
                    {'length': 50, 'value': 'Robert Goddard'},
                    {'length': 10, 'value': '1443'},
                    {'length': 5, 'value': '00002'}
                ],
                [
                    {'length': 9, 'value': 'S0981459J'},
                    {'length': 50, 'value': 'Katherine Johnson'},
                    {'length': 10, 'value': '9882'},
                    {'length': 5, 'value': '00003'}
                ]
            ],
            'footer': [{'length': 20, 'value': '00000000000010001001'}]
        }
        file_helper.create_multiline_fixed_width_file(source_file_content, f'{test_id}-source.txt')

        # Create reference data file
        ref_file_content = {
            'header': [
                {'length': 1, 'value': 'Y'},
                {'length': 8, 'value': '20200101'},
            ],
            'body': [
                [
                    {'length': 9, 'value': 'S0981459J'},
                    {'length': 50, 'value': 'Katherine Johnson'},
                    {'length': 10, 'value': '9882'},
                    {'length': 5, 'value': '00003'}
                ],
                [
                    {'length': 9, 'value': 'S2995883A'},
                    {'length': 50, 'value': 'John Hopkins'},
                    {'length': 10, 'value': '1000'},
                    {'length': 5, 'value': '00001'}
                ],
                [
                    {'length': 9, 'value': 'S5726556F'},
                    {'length': 50, 'value': 'Robert Goddard'},
                    {'length': 10, 'value': '1443'},
                    {'length': 5, 'value': '00002'}
                ]
            ],
            'footer': [{'length': 20, 'value': '000000000000100010'}]
        }
        ref_file_name = f'{test_id}-ref.txt'
        file_helper.create_multiline_fixed_width_file(ref_file_content, ref_file_name)
        bucket_name = 'integration-test'
        aws_helper.upload_s3_file(file_name=f'{test_id}-ref.txt', bucket=bucket_name, s3_file_key=ref_file_name)
        # Create source config
        config_text = """
        header:
          formatter: HeaderSourceFormatter
          format:
            - name: recordType
              spec: 0,1
            - name: reportGenerationDate
              spec: 1,9
        body:
          formatter: BodySourceFormatter
          format:
            - name: nric
              spec: 0,9
            - name: name
              spec: 9,59
            - name: amount
              spec: 59,69
            - name: internal_id
              spec: 69,74
        footer:
          formatter: FooterSourceFormatter
          format:
            - name: recordType
              spec: 0,20
        """
        config_dict = yaml.safe_load(config_text)
        source_mapper_cfg = SourceMapperConfig({'source': config_dict}, file_name=f'{test_id}-source.txt')
        # Create ref config
        file_helper.create_file(config_text, f'{test_id}-ref-cfg.yml')
        constraint = constraints.S3FileRefConstraint()

        mocker.patch.dict(os.environ, {'mount_path': project_root_path})
        constraint.run(source_mapper_cfg, arguments={
                'file_name': ref_file_name,
                'bucket': bucket_name,
                'format': f'{test_id}-ref-cfg.yml',
                'source_file_path': '/test/file.txt',
                'config': [{
                    'source_segment': 'header',
                    'ref_segment': 'header',
                    'fields': [{
                        'source': 'recordType',
                        'ref': 'recordType'
                        }]
                    }, {
                    'source_segment': 'body',
                    'ref_segment': 'body',
                    'source_aggregate': 'nric',
                    'ref_aggregate': 'nric',
                    'fields': [{
                        'source': 'name',
                        'ref': 'name'
                    }, {
                        'source': 'amount',
                        'ref': 'amount'
                    }]
                }]
            })

    def test_invalid_row_mismatch(self, test_id, mocker):
        # Create source data file

        source_file_content = {
            'header': [
                {'length': 1, 'value': 'Y'},
                {'length': 8, 'value': '20200101'},
            ],
            'body': [
                [
                    {'length': 9, 'value': 'S2995883A'},
                    {'length': 50, 'value': 'John Hopkins'},
                    {'length': 10, 'value': '1000'},
                    {'length': 5, 'value': '00001'}
                ],
                [
                    {'length': 9, 'value': 'S5726556F'},
                    {'length': 50, 'value': 'Robert Goddard'},
                    {'length': 10, 'value': '1443'},
                    {'length': 5, 'value': '00002'}
                ],
                [
                    {'length': 9, 'value': 'S0981459J'},
                    {'length': 50, 'value': 'Katherine Johnson'},
                    {'length': 10, 'value': '9882'},
                    {'length': 5, 'value': '00003'}
                ]
            ],
            'footer': [{'length': 20, 'value': '00000000000010001001'}]
        }
        file_helper.create_multiline_fixed_width_file(source_file_content, f'{test_id}-source.txt')

        # Create reference data file
        ref_file_content = {
            'header': [
                {'length': 1, 'value': 'Y'},
                {'length': 8, 'value': '20200101'},
            ],
            'body': [
                [
                    {'length': 9, 'value': 'S0981459J'},
                    {'length': 50, 'value': 'Katherine Johnson'},
                    {'length': 10, 'value': '9882'},
                    {'length': 5, 'value': '00003'}
                ],
                [
                    {'length': 9, 'value': 'S2995883A'},
                    {'length': 50, 'value': 'John Hopkins'},
                    {'length': 10, 'value': '1000'},
                    {'length': 5, 'value': '00001'}
                ]
            ],
            'footer': [{'length': 20, 'value': '000000000000100010'}]
        }
        ref_file_name = f'{test_id}-ref.txt'
        file_helper.create_multiline_fixed_width_file(ref_file_content, ref_file_name)
        bucket_name = 'integration-test'
        aws_helper.upload_s3_file(file_name=f'{test_id}-ref.txt', bucket=bucket_name, s3_file_key=ref_file_name)
        # Create source config
        config_text = """
        header:
          formatter: HeaderSourceFormatter
          format:
            - name: recordType
              spec: 0,1
            - name: reportGenerationDate
              spec: 1,9
        body:
          formatter: BodySourceFormatter
          format:
            - name: nric
              spec: 0,9
            - name: name
              spec: 9,59
            - name: amount
              spec: 59,69
            - name: internal_id
              spec: 69,74
        footer:
          formatter: FooterSourceFormatter
          format:
            - name: recordType
              spec: 0,20
        """
        config_dict = yaml.safe_load(config_text)
        source_mapper_cfg = SourceMapperConfig({'source': config_dict}, file_name=f'{test_id}-source.txt')
        # Create ref config
        file_helper.create_file(config_text, f'{test_id}-ref-cfg.yml')
        constraint = constraints.S3FileRefConstraint()

        mocker.patch.dict(os.environ, {'mount_path': project_root_path})
        with pytest.raises(exceptions.FailedConstraintsException):
            constraint.run(source_mapper_cfg, arguments={
                    'file_name': ref_file_name,
                    'bucket': bucket_name,
                    'format': f'{test_id}-ref-cfg.yml',
                    'source_file_path': '/test/file.txt',
                    'config': [{
                        'source_segment': 'body',
                        'ref_segment': 'body',
                        'fields': [{
                            'source': 'nric',
                            'ref': 'nric'
                        }]
                    }]
                })

    def test_invalid_data_mismatch(self, test_id, mocker):
        # Create source data file

        source_file_content = {
            'header': [
                {'length': 1, 'value': 'Y'},
                {'length': 8, 'value': '20200101'},
            ],
            'body': [
                [
                    {'length': 9, 'value': 'S2995883A'},
                    {'length': 50, 'value': 'John Hopkins'},
                    {'length': 10, 'value': '1000'},
                    {'length': 5, 'value': '00001'}
                ],
                [
                    {'length': 9, 'value': 'S5726556F'},
                    {'length': 50, 'value': 'Robert Goddard'},
                    {'length': 10, 'value': '1443'},
                    {'length': 5, 'value': '00002'}
                ],
                [
                    {'length': 9, 'value': 'S0981459J'},
                    {'length': 50, 'value': 'Katherine Johnson'},
                    {'length': 10, 'value': '9882'},
                    {'length': 5, 'value': '00003'}
                ]
            ],
            'footer': [{'length': 20, 'value': '00000000000010001001'}]
        }
        file_helper.create_multiline_fixed_width_file(source_file_content, f'{test_id}-source.txt')

        # Create reference data file
        ref_file_content = {
            'header': [
                {'length': 1, 'value': 'Y'},
                {'length': 8, 'value': '20200101'},
            ],
            'body': [
                [
                    {'length': 9, 'value': 'S0981459J'},
                    {'length': 50, 'value': 'Katherine Johnson'},
                    {'length': 10, 'value': '9882'},
                    {'length': 5, 'value': '00003'}
                ],
                [
                    {'length': 9, 'value': 'S2995883A'},
                    {'length': 50, 'value': 'John Hopkins'},
                    {'length': 10, 'value': '1000'},
                    {'length': 5, 'value': '00001'}
                ],
                [
                    {'length': 9, 'value': 'S5726556F'},
                    {'length': 50, 'value': 'Robert Goddard'},
                    {'length': 10, 'value': '1443'},
                    {'length': 5, 'value': '00002'}
                ],
            ],
            'footer': [{'length': 20, 'value': '000000000000100010'}]
        }
        ref_file_name = f'{test_id}-ref.txt'
        file_helper.create_multiline_fixed_width_file(ref_file_content, ref_file_name)
        bucket_name = 'integration-test'
        aws_helper.upload_s3_file(file_name=f'{test_id}-ref.txt', bucket=bucket_name, s3_file_key=ref_file_name)
        # Create source config
        config_text = """
        header:
          formatter: HeaderSourceFormatter
          format:
            - name: recordType
              spec: 0,1
            - name: reportGenerationDate
              spec: 1,9
        body:
          formatter: BodySourceFormatter
          format:
            - name: nric
              spec: 0,9
            - name: name
              spec: 9,59
            - name: amount
              spec: 59,69
            - name: internal_id
              spec: 69,74
        footer:
          formatter: FooterSourceFormatter
          format:
            - name: recordType
              spec: 0,20
        """
        config_dict = yaml.safe_load(config_text)
        source_mapper_cfg = SourceMapperConfig({'source': config_dict}, file_name=f'{test_id}-source.txt')
        # Create ref config
        file_helper.create_file(config_text, f'{test_id}-ref-cfg.yml')
        constraint = constraints.S3FileRefConstraint()

        mocker.patch.dict(os.environ, {'mount_path': project_root_path})
        with pytest.raises(exceptions.FailedConstraintsException):
            constraint.run(source_mapper_cfg, arguments={
                    'file_name': ref_file_name,
                    'bucket': bucket_name,
                    'format': f'{test_id}-ref-cfg.yml',
                    'source_file_path': '/test/file.txt',
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
