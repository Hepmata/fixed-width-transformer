import math
import os
import time
import re

import numpy as np
import pandas as pd
from transformer.library import exceptions, aws_service
from transformer.library import logger

log = logger.set_logger(__name__)


def check_environment_variables(variables: list):
    error_list = []
    data = []
    for variable in variables:
        try:
            data.append(os.environ[variable])
        except KeyError as e:
            error_list.append(variable)
    if len(error_list) > 0:
        raise KeyError(f'Environment variables {error_list} are missing')
    return data


def wait_for_file(max_wait_time, wait_interval, bucket, file_name):
    current_wait_time = 0
    while current_wait_time < max_wait_time:
        log.info(f'Still waiting for [Bucket: {bucket}, Key: {file_name}]. Elapsed Time: {current_wait_time}s')
        time.sleep(wait_interval)
        if aws_service.check_s3_file_exists(bucket, file_name, True):
            log.info(f'[Bucket: {bucket}, Key: {file_name}] found!')
            break
        current_wait_time += wait_interval

    if current_wait_time >= max_wait_time and not aws_service.check_s3_file_exists(bucket, file_name):
        raise exceptions.FailedConstraintException(
            msg=f'HashConstraint failed due to exceeding max_wait_time while waiting for file {file_name}')


def build_aws_endpoint_url(service_name: str = ''):
    valid_regions = [
        'us-east-1',
        'us-east-2',
        'us-west-1',
        'us-west-2',
        'ap-southeast-1',
        'ap-southeast-2',
        'ap-southeast-3'
    ]
    region = os.environ['region']
    if region not in valid_regions:
        raise ValueError(f'region {region} is not valid. Supported values are {valid_regions}')
    if service_name == '':
        return f"https://{os.environ['region']}.amazonaws.com"
    else:
        return f"https://{service_name}.{os.environ['region']}.amazonaws.com"


def file_name_builder(source_file_name: str, file_config: dict, dataframe=None):
    matches = list(re.findall(file_config['extraction_regex'], source_file_name)[0])
    if len(matches) == 0:
        raise ValueError(f"No matches found matching extraction regex [{file_config['extraction_regex']}]")

    if dataframe is not None:
        file_config['result_file_name'] = file_name_frame_builder(file_config, dataframe)
    if type(file_config['replacement_text']) == str:
        file_number = matches[0]
        result_file_name = file_config['result_file_name'].replace(file_config['replacement_text'], file_number)
    elif type(file_config['replacement_text']) == list:
        if len(file_config['replacement_text']) != len(matches):
            raise ValueError(f'Replace length does not match the regex matches')
        result_file_name = file_config['result_file_name']
        for replace, match in zip(file_config['replacement_text'], matches):
            result_file_name = result_file_name.replace(replace, match)

    return result_file_name


def file_name_frame_builder(file_config: dict, dataframe=None):
    file_name = str(file_config['result_file_name'])
    while file_name.find('$') != -1:
        start = file_name.find('$') + 2
        temp_file_name = file_name[start::]
        end = temp_file_name.find('}')
        replace_word = temp_file_name[0:end]
        first_segment, second_segment = replace_word.split('.')
        if '[' in second_segment:
            split_range = second_segment.split('[')[1][0:-1].split(',')
            start, end = int(split_range[0]), int(split_range[1])
            file_name = file_name.replace('${' + replace_word + '}',
                                          dataframe[first_segment][second_segment.split('[')[0]][0][start:end])
        else:
            file_name = file_name.replace('${' + replace_word + '}', dataframe[first_segment][second_segment][0])
    return file_name


def get_failed_record_count(source_df: pd.Series, ref_df: pd.Series):
    if len(source_df.index) != len(ref_df.index):
        raise ValueError(
            f'source_df and ref_df must be of equal length, but received {len(source_df.index)} to {len(ref_df.index)}')

    results = source_df == ref_df
    true_count = np.count_nonzero(results)
    return len(source_df.index) - true_count


def get_success_record_count(source_df: pd.Series, ref_df: pd.Series):
    if len(source_df.index) != len(ref_df.index):
        raise ValueError(
            f'source_df and ref_df must be of equal length, but received {len(source_df.index)} to {len(ref_df.index)}')

    results = source_df == ref_df
    true_count = np.count_nonzero(results)
    return true_count


def convert_sqs_arn_to_url(arn: str):
    try:
        splits = arn.split(':')
        name = splits[-1]
        acc_no = splits[-2]
        url = f"{build_aws_endpoint_url('sqs')}/{acc_no}/{name}"
        return url
    except IndexError as e:
        raise ValueError(f'Failed to parse arn due to exception: {e}')
    except AttributeError as e:
        raise ValueError(f'Failed to parse arn due to exception: {e}')


def convert_size(size_bytes):
    if type(size_bytes) == str:
        size_bytes = int(size_bytes)
    if size_bytes == 0:
        return '0B'
    size_name = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return '%s %s' % (s, size_name[i])
