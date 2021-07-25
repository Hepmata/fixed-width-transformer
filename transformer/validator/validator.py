import dataclasses

import pandas as pd

from transformer.library import logger
from transformer.library.exceptions import ValidationError, MissingConfigError
import sys

log = logger.set_logger(__name__)
module = sys.modules[__name__]


class AbstractValidator:
    def validate(self,
                 segment: str,
                 field_name: str,
                 arguments: dict,
                 frames: dict[pd.DataFrame]
                 ): pass


class NricValidator(AbstractValidator):
    def validate(self,
                 segment: str,
                 field_name: str,
                 arguments: dict,
                 frames: dict[str, pd.DataFrame]
                 ):
        target_series = frames[segment][field_name]

        matched = target_series[target_series.str.count(r'(?i)^[STFG]\d{7}[A-Z]$') == True]
        if len(matched.index) != len(target_series.index):
            failure_count = target_series.size - matched.size
            raise ValidationError(
                f"NRIC Validation Failure for {segment}, {field_name} with {failure_count}/{target_series.size} count",
                segment,
                field_name,
                failure_count,
                target_series.size
            )


class RegexValidator(AbstractValidator):
    def validate(self,
                 segment: str,
                 field_name: str,
                 arguments: dict,
                 frames: dict[str, pd.DataFrame]
                 ):
        if 'pattern' not in arguments.keys():
            raise MissingConfigError("Required argument [pattern] is missing. Please verify configuration.")

        if not isinstance(arguments['pattern'], str):
            raise MissingConfigError(
                "Required argument [pattern] is not of string/str type. Please verify configuration")

        target_series = frames[segment][field_name]
        matched = target_series[target_series.str.count(arguments['pattern']) == True]

        if len(matched.index) != len(target_series.index):
            failure_count = target_series.size - matched.size
            raise ValidationError(
                f"Regex Validation Failure for {segment}, {field_name} with {failure_count}/{target_series.size} count",
                segment,
                field_name,
                failure_count,
                target_series.size
            )


class NaNValidator(AbstractValidator):

    def validate(self,
                 segment: str,
                 field_name: str,
                 arguments: dict,
                 frames: dict[str, pd.DataFrame]
                 ):
        target_frame = frames[segment]
        if field_name.upper() == "ALL":
            result = target_frame.isnull().values.any()
        else:
            result = target_frame[field_name].isnull().values.any()
        if result:
            raise ValidationError(
                "Failed NaN Validation. Please check file and source config.",
                segment,
                field_name,
                len(target_frame.index),
                len(target_frame.index)
            )


class RefValidator(AbstractValidator):
    def validate(self,
                 segment: str,
                 field_name: str,
                 arguments: dict,
                 frames: dict[str, pd.DataFrame]
                 ):
        if arguments['type'] == "match":
            splits = arguments['ref'].split('.')
            target = frames[splits[0]][splits[1]]
            source = frames[segment][field_name]
            print(target, source)

            if not target.equals(source):
                raise ValidationError(
                    "Failed RefValidation",
                    segment,
                    field_name,
                    target.value_counts().loc[False],
                    len(target.index)
                )

        if arguments['type'] == "count":
            splits = arguments['ref'].split('.')
            target_count = len(frames[segment][field_name].index)
            if target_count == 1:
                # Reversed Flow
                target_count = len(frames[splits[0]][splits[1]].index)
                expected_count = frames[segment][field_name][0]
            else:
                expected_count = frames[splits[0]][splits[1]][0]
            if int(target_count) != int(expected_count):
                raise ValidationError(
                    "Failed RefValidation",
                    segment,
                    field_name,
                    int(target_count),
                    int(expected_count)
                )
