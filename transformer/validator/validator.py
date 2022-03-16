import re
import datetime
import pandas
import pandas as pd
from typing import Dict
from mysql import connector
from pytz import timezone
import transformer.library.logger as logger
import transformer.library.aws_service as aws_service
from transformer.library.exceptions import (
    ValidationError,
    MissingConfigError,
    InvalidConfigError,
)
import sys

log = logger.set_logger(__name__)
module = sys.modules[__name__]


class AbstractValidator:
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        pass


class SqlValidator(AbstractValidator):
    """
    Arguments
    ---------
    secret_name: str
        Secret Name of secret stored in AWS SecretsManager containing the username/password
        to the SQL server
        Secret contents must be of json type and contains the fields "username" and "password"
    sql_language: str
        SQL Language to be used, can only be MySQL for now.
    sql_query: str
        SQL Query to be executed, must be a valid query and only returns 1 row.
        Example: SELECT name FROM db.names WHERE name = "john" LIMIT 1;
    """

    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        cnx = self.connect(arguments)
        sql_dataframe = pd.read_sql(arguments['sql_query'], cnx)
        cnx.close()
        if not frames[segment][field_name].equals(sql_dataframe[field_name]):
            raise ValidationError(
                f'Failed {self.__class__.__name__} for field {segment}.{field_name}',
                type(self).__name__,
                segment,
                field_name,
                0,
                frames[segment][field_name].size,
            )

    def connect(self, arguments):
        secret = aws_service.retrieve_secret(arguments['secret_name'])
        cnx = connector.connect(
            user=secret['username'],
            password=secret['password'],
            host=arguments['host'],
            database=arguments['database'],
        )
        return cnx


class UniqueValueValidator(AbstractValidator):
    def validate(
            self,
            segment: str,
            field_name: str,
            arguments: dict,
            frames: Dict[str, pd.DataFrame],
    ):
        target_df = frames[segment][field_name]
        yes = pandas.Series(target_df).is_unique
        if not yes:
            failure_count = len(target_df.index) - pandas.Series(target_df).nunique()
            raise ValidationError(
                f'{failure_count}/{len(target_df.index)} records failed UniqueValueValidator for field {segment}.{field_name}',
                type(self).__name__,
                segment,
                field_name,
                int(failure_count),
                len(target_df.index),
            )


class RegexValidator(AbstractValidator):
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        if 'pattern' not in arguments.keys():
            raise MissingConfigError(
                'Required argument [pattern] is missing. Please verify configuration.'
            )
        if not isinstance(arguments['pattern'], str):
            raise MissingConfigError(
                'Required argument [pattern] is not of string/str type. Please verify configuration'
            )
        if 'type' not in arguments.keys():
            arguments['type'] = 'positive'
        if arguments['type'] not in ['positive', 'negative']:
            raise MissingConfigError(
                'Required argument [type] value must be either positiev or negative'
            )
        target_series = frames[segment][field_name]
        if arguments['type'] == 'positive':
            matched = target_series[
                target_series.str.count(arguments['pattern']) == True
            ]
        else:
            matched = target_series[
                target_series.str.count(arguments['pattern']) == False
            ]
        if len(matched.index) != len(target_series.index):
            failure_count = target_series.size - matched.size
            raise ValidationError(
                f"{failure_count}/{target_series.size} records failed RegexValidator for field {segment}.{field_name} with pattern {arguments['pattern']}",
                type(self).__name__,
                segment,
                field_name,
                failure_count,
                target_series.size,
            )


class NaNValidator(AbstractValidator):
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        target_frame = frames[segment]
        if field_name.upper() == 'ALL':
            result = target_frame.isnull().values.any()
        else:
            result = target_frame[field_name].isnull().values.any()
        if result:
            raise ValidationError(
                'Failed NaN Validation. Please check file and source config.',
                type(self).__name__,
                segment,
                field_name,
                len(target_frame.index),
                len(target_frame.index),
            )

class NaNOnlyValidator(AbstractValidator):
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        target_frame = frames[segment]
        if field_name.upper() == 'ALL':
            result = target_frame.isnull().values.any()
        else:
            result = target_frame[field_name].isnull().values.any()
        if not result:
            raise ValidationError(
                'Failed NaN Only Validation. Please check file and source config.',
                type(self).__name__,
                segment,
                field_name,
                len(target_frame.index),
                len(target_frame.index),
            )

class DuplicateGroupedValidator(AbstractValidator):
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        target_frame = frames[segment]

        df = pd.DataFrame(
            {
                field_name: frames[segment][field_name],
                arguments['ref']: frames[segment][arguments['ref']],
            }
        )
        df['duplicated'] = df.duplicated([field_name, arguments['ref']])
        duplicate_count = df['duplicated'].sum()
        if duplicate_count > 0:
            raise ValidationError(
                f"{duplicate_count} records failed DuplicateGroupedValidator in field {segment}.{field_name} aggregated with {arguments['ref']}",
                type(self).__name__,
                segment,
                field_name,
                int(duplicate_count),
                len(df.index),
            )


class TotalReferenceValidator(AbstractValidator):
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        reference_splits = arguments['ref'].split('.')
        target_frame = frames[segment][field_name]
        try:
            target_amount = pd.to_numeric(target_frame).sum()
            reference_amount = pd.to_numeric(
                frames[reference_splits[0]][reference_splits[1]]
            ).sum()
        except ValueError as e:
            raise ValidationError(
                f'TotalReferenceValidator validation error: {segment}.{field_name} contains non digit values',
                type(self).__name__,
                segment,
                field_name,
                len(target_frame.index),
                len(target_frame.index),
            )
        if target_amount != reference_amount:
            raise ValidationError(
                f"{int(target_amount)}/{int(reference_amount)} amount failed TotalReferenceValidator does not match for fields: {segment}.{field_name} referencing against {arguments['ref']}",
                type(self).__name__,
                segment,
                field_name,
                len(target_frame.index),
                len(target_frame.index),
            )


class RefValidator(AbstractValidator):
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        if arguments['type'] == 'match':
            splits = arguments['ref'].split('.')
            target = frames[splits[0]][splits[1]]
            source = frames[segment][field_name]
            if not target.equals(source):
                raise ValidationError(
                    f"{len(source.index)}/{len(target.index)} records failed RefValidation for field: {segment}.{field_name} referencing against {arguments['ref']}",
                    type(self).__name__,
                    segment,
                    field_name,
                    len(source.index),
                    len(target.index),
                )

        if arguments['type'] == 'count':
            splits = arguments['ref'].split('.')
            target_count = len(frames[segment][field_name].index)
            if target_count == 1:
                # Reversed Flow
                target_count = int(len(frames[splits[0]][splits[1]].index))
                expected_count = int(frames[segment][field_name][0])
            else:
                expected_count = int(frames[splits[0]][splits[1]][0])
            if target_count != expected_count:
                raise ValidationError(
                    f"{expected_count}/{target_count} records failed RefValidation for field: {segment}.{field_name} referencing against {arguments['ref']}",
                    type(self).__name__,
                    segment,
                    field_name,
                    target_count,
                    expected_count,
                )


class DateValidator(AbstractValidator):
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        def check_date(date_str: str, date_tz: timezone):
            try:
                retrieved_date = datetime.datetime.strptime(
                    date_str, arguments['format']
                )
                retrieved_date = retrieved_date.replace(tzinfo=date_tz).date()
                current_date = datetime.datetime.now(date_tz).date()
                if arguments['mode'] == 'future':
                    if retrieved_date > current_date:
                        return True
                elif arguments['mode'] == 'yesterday':
                    yesterday_date = current_date - datetime.timedelta(days=1)
                    if retrieved_date == yesterday_date:
                        return True
                elif arguments['mode'] == 'today':
                    if current_date == retrieved_date:
                        return True
                elif arguments['mode'] == 'valid':
                    return True
                else:
                    raise InvalidConfigError(
                        f'DateValidator for {segment}:{field_name} is invalid; unknown mode {arguments["mode"]}'
                    )
                return False
            except ValueError as e:
                return False

        if 'format' not in arguments.keys():
            arguments['format'] = '%Y%m%d'
        if 'mode' not in arguments.keys():
            arguments['mode'] = 'valid'
        if 'timezone' not in arguments.keys():
            date_tz = 'Asia/Singapore'
        else:
            if not re.match('\w+\/\w+', arguments['timezone']):
                raise InvalidConfigError(
                    msg=f'DateValidator for {segment}:{field_name} is invalid. Please check tzlocal documentation'
                )
            date_tz = arguments['timezone']
        target_frame = frames[segment][field_name]
        target_count = len(target_frame.index)
        success_amount = target_frame.apply(
            lambda x: check_date(x, timezone(date_tz))
        ).sum()
        failure_amount = target_count - success_amount
        if success_amount != target_count:
            raise ValidationError(
                f'{failure_amount}/{target_count} records failed DateValidation for field: {segment}.{field_name}',
                type(self).__name__,
                segment,
                field_name,
                int(failure_amount),
                int(target_count),
            )


class MinimumAmountValidator(AbstractValidator):
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        expected_frame = frames[segment][field_name]
        expected_count = len(expected_frame.index)
        target_frame = expected_frame[
            pd.to_numeric(expected_frame) >= arguments['amount']
        ]
        target_count = len(target_frame.index)
        if expected_count != target_count:
            raise ValidationError(
                f'{expected_count-target_count}/{expected_count} records failed MinimumAmountValidation for field: {segment}.{field_name}',
                type(self).__name__,
                segment,
                field_name,
                int(expected_count - target_count),
                int(expected_count),
            )


class ConditionalRefValidator(AbstractValidator):
    def validate(
        self,
        segment: str,
        field_name: str,
        arguments: dict,
        frames: Dict[str, pd.DataFrame],
    ):
        if 'criterion' not in arguments.keys():
            raise MissingConfigError(
                'Required argument [criterion] is missing. Please verify configuration.'
            )
        if 'condition' not in arguments.keys():
            raise MissingConfigError(
                'Required argument [condition] is missing. Please verify configuration.'
            )
        if arguments['condition'] not in ['equal', 'not equal']:
            raise MissingConfigError(
                'Required argument [condition] value must be either equal or not equal'
            )
        if 'value' not in arguments.keys():
            raise MissingConfigError(
                'Required argument [value] is missing. Please verify configuration.'
            )
        if 'type' not in arguments.keys():
            raise MissingConfigError(
                'Required argument [type] is missing. Please verify configuration.'
            )
        if arguments['type'] not in ['count', 'sum']:
            raise MissingConfigError(
                'Required argument [type] value must be either count or sum'
            )
        reference_splits = arguments['ref'].split('.')
        criterion_splits = arguments['criterion'].split('.')
        reference_df = pd.DataFrame(frames[reference_splits[0]][reference_splits[1]])
        criterion_df = pd.DataFrame(frames[criterion_splits[0]][criterion_splits[1]])
        merged_df = reference_df.merge(criterion_df, left_index=True, right_index=True)
        if arguments['condition'] == 'equal':
            matched_df = merged_df[merged_df[criterion_splits[1]] == arguments['value']]
        elif arguments['condition'] == 'not equal':
            matched_df = merged_df[merged_df[criterion_splits[1]] != arguments['value']]
        dataFrames = {f'{reference_splits[0]}': matched_df, f'{segment}': pd.DataFrame(frames[segment])}
        if arguments['type'] == 'count':
            new_arguments = {'type': 'count', 'ref': f'{reference_splits[0]}.{reference_splits[1]}'}
            RefValidator().validate(segment, field_name, new_arguments, dataFrames)
        elif arguments['type'] == 'sum':
            new_arguments = {'ref': f'{reference_splits[0]}.{reference_splits[1]}'}
            TotalReferenceValidator().validate(segment, field_name, new_arguments, dataFrames)
