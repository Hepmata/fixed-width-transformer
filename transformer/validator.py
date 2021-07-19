import pandas as pd
from transformer.config import ValidatorConfig
from transformer.library import logger
from transformer.library.exceptions import ValidationError, MissingConfigError
import sys

log = logger.set_logger(__name__)
module = sys.modules[__name__]

# def validate_records(validation_config, frames):
#     failed_validation = []
#     for key in validation_config.keys():
#         segment = validation_config[key]['segment']
#         for validator in validation_config[key]['validators']:
#             log.info(f"Running validator for field [{key}] with validation config: {validator}")
#             target_frame = pd.Series(frames[segment][key])
#             if validator['name'] == NricValidator.__name__:
#                 response = getattr(module, validator['name'])(target_frame).validate()
#             elif validator['name'] == RefValidator.__name__:
#                 log.info("Applying custom trigger for module")
#                 response = getattr(module, validator['name'])(frames, {
#                     'key': key,
#                     'name': validator['name'],
#                     'ref': validator['ref'],
#                     'type': validator['type'],
#                     'segment': segment
#                 }).validate()
#             else:
#                 response = getattr(module, validator['name'])(target_frame, validator['pattern']).validate()
#             if response['result']:
#                 log.info(f"Validation for field [{key}] passed!")
#             else:
#                 failed_validation.append({
#                     'field': key,
#                     'validator': validator,
#                     'count': response['count']
#                 })
#                 print(failed_validation)
#     if len(failed_validation) > 0:
#         raise ValidationError(f"Failed to validate the following fields {failed_validation}. Failure Count is [{failed_validation}]")

class AbstractValidator:
    config: ValidatorConfig

    def __init__(self, config: ValidatorConfig):
        self.config = config

    def validate(self, frames: dict): pass


class NricValidator(AbstractValidator):

    def __init__(self, config: ValidatorConfig):
        super().__init__(config)

    def validate(self, frames: dict):
        target_series = frames[self.config.segment][self.config.field_name]

        matched = target_series[target_series.str.count(r'(?i)^[STFG]\d{7}[A-Z]$') == True]
        if len(matched.index) != len(target_series.index):
            raise ValidationError(
                "Validation Failed",
                self.config.segment,
                self.config.field_name,
                matched.size,
                target_series.size
            )


class RegexValidator(AbstractValidator):
    """
    :param frame
    :param pattern
    Validates content based on provided pattern. Can be triggered by putting in a validator segment in config.yaml with [name] and [pattern].
    Example:
    - name: recordType
      spec: 0,1
      validator:
        name: regex_validator
        pattern: someregexpattern
    Exceptions:
    Returns TypeError when None type is given.
    Responses:
    Returns True if regex matches
    Returns False if regex does not match
    """

    def __init__(self, config: ValidatorConfig):
        super().__init__(config)

    def validate(self, frames: dict):
        if 'pattern' not in self.config.arguments.keys():
            raise MissingConfigError("Required argument [pattern] is missing. Please verify configuration.")

        if not isinstance(self.config.arguments['pattern'], str):
            raise MissingConfigError("Required argument [pattern] is not of string/str type. Please verify configuration")

        target_series = frames[self.config.segment][self.config.field_name]
        matched = target_series[target_series.str.count(self.config.arguments['pattern']) == True]

        if len(matched.index) != len(target_series.index):
            raise ValidationError(
                "Validation Failed",
                self.config.segment,
                self.config.field_name,
                matched.size,
                target_series.size
            )


class NaNValidator(AbstractValidator):

    def __init__(self, config: ValidatorConfig):
        super().__init__(config)

    def validate(self, frames: dict):
        target_frame = frames[self.config.segment]
        if self.config.field_name.upper() == "ALL":
            result = target_frame.isnull().values.any()
        else:
            result = target_frame[self.config.field_name].isnull().values.any()
        if result:
            raise ValidationError(
                "Failed NaN Validation. Please check file and source config.",
                self.config.segment,
                self.config.field_name,
                len(target_frame.index),
                len(target_frame.index)
            )


class RefValidator(AbstractValidator):
    def __init__(self, config: ValidatorConfig):
        super().__init__(config)

    def validate(self, frames: dict):
        if self.config.arguments['type'] == "match":
            splits = self.config.arguments['ref'].split('.')
            target = frames[splits[0]][splits[1]]
            source = frames[self.config.segment][self.config.field_name]
            print(target, source)

            if not target.equals(source):
                raise ValidationError(
                    "Failed RefValidation",
                    self.config.segment,
                    self.config.field_name,
                    target.value_counts().loc[False],
                    len(target.index)
                )

        if self.config.arguments['type'] == "count":
            splits = self.config.arguments['ref'].split('.')
            target_count = len(frames[self.config.segment][self.config.field_name].index)
            if target_count == 1:
                # Reversed Flow
                target_count = len(frames[splits[0]][splits[1]].index)
                expected_count = frames[self.config.segment][self.config.field_name][0]
            else:
                expected_count = frames[splits[0]][splits[1]][0]
            if int(target_count) != int(expected_count):
                raise ValidationError(
                    "Failed RefValidation",
                    self.config.segment,
                    self.config.field_name,
                    int(target_count),
                    int(expected_count)
                )
