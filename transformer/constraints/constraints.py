import dataclasses
import datetime
import json
import os
import re
import typing
from io import StringIO

import numpy as np
import pandas as pd
import yaml

from transformer.constraints.constraints_config import SegmentCheckRule
from transformer.library import aws_service, exceptions, common
import transformer.library.logger as logger
import mysql.connector as connector
from transformer.source import SourceMapperConfig, SourceMapper

log = logger.set_logger(__name__)


class AbstractConstraint:
    def run(self, config: SourceMapperConfig, arguments: dict):
        self.validate_arguments(arguments)

    def validate_arguments(self, arguments: dict): pass


class HashConstraint(AbstractConstraint):
    """
    Arguments
    ---------
    max_wait_time: float
    wait_interval: float
    source_file_path: str
        Path to source data file. This is provided by the system.
    bucket: str
        S3 Bucket containing the hash file. Compulsory field
    file_name: str
        S3 File Name or Key of the hash file. Compulsory field
    algorithm: str
        Algorithm used in the Hash file
    """

    def run(self, config: SourceMapperConfig, arguments: dict):
        super().run(config, arguments)
        hash_bucket = arguments['bucket']
        source_file_path = arguments['source_file_path']
        if 'file_name' in arguments.keys():
            hash_file_name = arguments['file_name']
        else:
            if 'file_config' not in arguments.keys():
                raise exceptions.ConstraintMisconfigurationException(f'Either file_name or file_config must be provided in the arguments for {self.__class__.__name__}')
            hash_file_name = common.file_name_builder(source_file_path.split('/')[-1], arguments['file_config'])
        max_wait_time = float(arguments['max_wait_time']) if type(arguments['max_wait_time']) == str else arguments['max_wait_time']
        wait_interval = float(arguments['wait_interval']) if type(arguments['wait_interval']) == str else arguments['wait_interval']

        # Wait for hash file to be available
        common.wait_for_file(max_wait_time=max_wait_time, wait_interval=wait_interval, bucket=hash_bucket,
                             file_name=hash_file_name)
        if 'mount_path' in os.environ.keys():
            hash_file_path = f"{os.environ['mount_path']}/{hash_file_name.split('/')[-1]}"
        else:
            hash_file_path = f'/tmp/{hash_file_name}'
        try:
            aws_service.download_s3_file(
                bucket=hash_bucket,
                key=hash_file_name,
                file_name=hash_file_path,
                date_check=True
            )
        except Exception as e:
            raise exceptions.FailedConstraintException(
                segment='NA',
                recordCount=0,
                failCount=0,
                msg=f'Failed to download hash file. Please check the the file exists and date is matching to the source file.'
            )

        request_payload = {
            'absolute_path': source_file_path,
            'algorithm': arguments['algorithm']
        }
        if 'use_line_ending_conversion' in arguments.keys():
            request_payload['use_line_ending_conversion'] = 'true'

        response = aws_service.invoke_lambda(
            function_name=f'FileTransformerHash',
            payload=json.dumps(request_payload)
        )

        if response['statusCode'] != 200:
            raise exceptions.FailedConstraintException(
                segment='NA',
                failCount=1,
                recordCount=1,
                msg=f"Failed to get hash from FileTransformerHash. StatusCode is {response['statusCode']} with message {response['payload']}")

        with open(hash_file_path, 'r') as file:
            file_content = file.read()
            file_content = file_content.strip()
            if file_content != response['body']:
                raise exceptions.FailedConstraintException(
                    segment='NA',
                    recordCount=1,
                    failCount=0,
                    msg=f"Failed HashConstraint with different hashes. Expected Hash: {file_content}, Received Hash: {response['payload']}"
                )

    def validate_arguments(self, arguments: dict):
        for field in ['bucket', 'algorithm', 'source_file_path', 'max_wait_time', 'wait_interval']:
            if field not in arguments.keys():
                raise exceptions.ConstraintMisconfigurationException(
                    f'Missing required argument {field} when calling {self.__class__.__name__}')


class SqlConstraint(AbstractConstraint):
    """
    Arguments
    ---------
    secret_name: str
        Secret name containing the username/password combo to your sql server
    database_host: str
         Database Host, should be a valid host
    database_port: str
         Database Port, should be a valid number value
    database_name: str
        Database Name that you are trying to connect to
    query: str
        SQL Query to run, use of arguments is allowed and should refer to colname specified below. If you want to refer
        to the source file name, please use the reserved name of "file_name"
        Example: "SELECT name from tbl_items WHERE id == {id}"
    source_data_segment: str [Optional]
        Segment of data to use for source mapping, only supports header or file_name.
        Example: header
    source_colnames: [Optional]
        column names to map the segment data with an appropriate name.
        Example: id,name,created_at
    source_colspecs: str [Optional]
        colspecs to map the segment data. specs should be in brackets, comma seperated. Number
        of colspecs must be the same as colnames.
        Example: (0,5),(5,10)
    source_date_format: [Optional]
        If using any date value parsed from file, this field is compulsory for the system to format it.
    """
    def run(self, config: SourceMapperConfig, arguments: dict):
        super().run(config, arguments)
        port = 3306 if 'database_port' not in arguments.keys() else arguments['database_port']
        source_date_format = '0' if 'source_date_format' not in arguments.keys() else arguments['source_date_format']
        conn = self.__connect__(
            secret_name=arguments['secret_name'],
            database_host=arguments['database_host'],
            database_port=port,
            database_name=arguments['database_name']
        )
        cursor = conn.cursor(dictionary=True)
        stripped_filename = self._get_filename(arguments)
        if 'source_data_segment' in arguments.keys():
            dataframe = self._get_dataframe(arguments)
            query = self.construct_query(
                query=arguments['query'],
                filename=stripped_filename,
                source_date_format=source_date_format,
                dataframe=dataframe)
        else:
            query = self.construct_query(
                query=arguments['query'],
                filename=stripped_filename,
                source_date_format=source_date_format,
                dataframe=pd.DataFrame())
        cursor.execute(query)
        count = cursor.fetchone()
        if count:
            conn.close()
            return

        conn.close()
        raise exceptions.FailedConstraintException(
            segment='NA',
            recordCount=1,
            failCount=1,
            msg=f'Failed {self.__class__.__name__} with no matches in database.')

    def construct_query(self, query: str, filename: str, source_date_format: str, dataframe: pd.DataFrame):
        matches = re.findall(r'\{[a-zA-Z0-9_]*\}', query)
        for match in matches:
            extracted_variable_name = match[1:-1]
            if extracted_variable_name == 'file_name':
                query = query.replace(match, filename)
            else:
                try:
                    extracted_date = datetime.datetime.strptime(str(dataframe[extracted_variable_name][0]), source_date_format)
                    query = query.replace(match, extracted_date.strftime('%Y-%m-%d'))
                except ValueError:
                    query = query.replace(match, str(dataframe[extracted_variable_name][0]))
        return query

    def _get_filename(self, arguments: dict):
        filename = arguments['source_file_path']
        return filename.split('/')[-1]

    def _get_dataframe(self, arguments: dict):
        colspecs = []
        colnames = []
        for colname in arguments['source_colnames'].split(','):
            colnames.append(colname)
        matches = re.findall(r'\(\s*\d+\s*,\s*[0-9]+\s*\)', arguments['source_colspecs'])
        for match in matches:
            match_splits = match.strip()[1:-1].split(',')
            colspecs.append((int(match_splits[0].strip()), int(match_splits[1].strip())))

        with open(arguments['source_file_path'], 'r') as file:
            return pd.read_fwf(
                StringIO(file.readline()),
                colspecs=colspecs,
                names=colnames,
                converters={h: str for h in colnames}
            )

    def __connect__(self, secret_name: str, database_host: str, database_port, database_name: str):
        credentials = aws_service.retrieve_secret(secret_name)
        if type(database_port) == str:
            database_port = int(database_port)
        cnx = connector.connection.MySQLConnection(
            user=credentials['username'],
            password=credentials['password'],
            host=database_host,
            port=database_port,
            database=database_name
        )
        return cnx

    def validate_arguments(self, arguments: dict):
        missing_fields = []
        for field in ['secret_name', 'database_host', 'database_name', 'query', 'source_file_path']:
            if field not in arguments:
                missing_fields.append(field)
        if len(missing_fields) > 0:
            raise exceptions.ConstraintMisconfigurationException(f'Required arguments {missing_fields} are missing for Constraint {self.__class__.__name__}')


class S3FileRefConstraint(AbstractConstraint):
    """
    This is a very complicated constraint that checks against another file using defined format.
    """
    external_format_config: SourceMapperConfig
    source_dataframes: typing.Dict[str, pd.DataFrame]
    ref_dataframes: typing.Dict[str, pd.DataFrame]

    def run(self, config: SourceMapperConfig, arguments: dict):
        super().run(config, arguments)
        external_format = arguments['format']
        source_file_path = arguments['source_file_path']
        # Map Source File
        self.source_dataframes = SourceMapper().run(config)
        # Map Data File
        self.external_format_config = self.map_external_config(external_format)
        if 'file_name' in arguments.keys():
            reference_file_name = arguments['file_name']
        else:
            if 'file_config' not in arguments.keys():
                raise exceptions.ConstraintMisconfigurationException(f'Either file_name or file_config must be provided in the arguments for {self.__class__.__name__}')
            reference_file_name = common.file_name_builder(source_file_path.split('/')[-1], arguments['file_config']
                                                           , self.source_dataframes)
        self.ref_dataframes = self.map_external_dataframes(arguments['bucket'], reference_file_name)
        # Compare
        self.compare(arguments)

    def map_external_config(self, filename: str):
        with open(filename, 'r') as file:
            config_dict = yaml.safe_load(file)
            config_dict = {
                'source': config_dict
            }
            return SourceMapperConfig(config_dict, '')

    def map_external_dataframes(self, bucket: str, key: str):
        if 'mount_path' in os.environ.keys():
            file_path = f"{os.environ['mount_path']}/{key.split('/')[-1]}"
        else:
            file_path = f"/tmp/{key.split('/')[-1]}"
        aws_service.download_s3_file(bucket, key, file_path)
        self.external_format_config.file_name = file_path
        return SourceMapper().run(self.external_format_config)

    def map_check_rules(self, arguments):
        configs = []
        for s in iter(arguments['config']):
            configs.append(SegmentCheckRule(s))
        return configs

    def compare(self, arguments):
        configs = self.map_check_rules(arguments)
        print(configs)
        errors = []
        for config in configs:
            if len(self.source_dataframes[config.source_segment].index) != len(self.ref_dataframes[config.ref_segment].index):
                errors.append(exceptions.FailedConstraintException(
                    segment=config.source_segment,
                    recordCount=len(self.source_dataframes[config.source_segment].index),
                    failCount=len(self.source_dataframes[config.source_segment].index) - len(self.ref_dataframes[config.ref_segment].index),
                    msg=f'Length of Dataframes do not match. Segment {config.source_segment} [count:{len(self.source_dataframes[config.source_segment].index)}] Segment {config.ref_segment} [count:{len(self.ref_dataframes[config.ref_segment].index)}]'))
            else:
                if config.has_aggregate():
                    self.source_dataframes[config.source_segment] = self.source_dataframes[config.source_segment].sort_values(
                        config.source_aggregate)
                    self.ref_dataframes[config.ref_segment] = self.ref_dataframes[config.ref_segment].sort_values(
                        config.ref_aggregate)
                source_values = None
                ref_values = None
                for ma in config.get_source_field_array():
                    if source_values is None:
                        source_values = self.source_dataframes[config.source_segment][ma].copy(True)
                    else:
                        source_values += self.source_dataframes[config.source_segment][ma].copy(True)
                for ma in config.get_ref_field_array():
                    if ref_values is None:
                        ref_values = self.ref_dataframes[config.ref_segment][ma].copy(True)
                    else:
                        ref_values += self.ref_dataframes[config.ref_segment][ma].copy(True)
                if not np.array_equiv(source_values, ref_values):
                    errors.append(exceptions.FailedConstraintException(
                        segment=config.source_segment,
                        recordCount=len(source_values.index),
                        failCount=common.get_failed_record_count(source_values, ref_values),
                        msg='Source/Ref values do not match}'))
                else:
                    log.info(f'Passed {self.__class__.__name__} for Source {config.source_segment} to Source {config.ref_segment}')
        if len(errors) > 0:
            raise exceptions.FailedConstraintsException(f'Failed Constraint check with {len(errors)}', errors)

    def validate_arguments(self, arguments: dict):
        pass
