import re
import pandas as pd
from library.exceptions import ValidationError
from library import logger
import sys

log = logger.set_logger(__name__)
module = sys.modules[__name__]

def pre_validate(): pass

def post_validate(): pass

def validate_records(validation_config, frames):
    failed_validation = []
    for key in validation_config.keys():
        segment = validation_config[key]['segment']
        for validator in validation_config[key]['validators']:
            log.info(f"Running validator for field [{key}] with validation config: {validator}")
            target_frame = pd.Series(frames[segment][key])
            if validator['name'] == NricValidator.__name__:
                response = getattr(module, validator['name'])(target_frame).validate()
            elif validator['name'] == RefValidator.__name__:
                log.info("Applying custom trigger for module")
                response = getattr(module, validator['name'])(frames, {
                    'key': key,
                    'name': validator['name'],
                    'ref': validator['ref'],
                    'type': validator['type'],
                    'segment': segment
                }).validate()
            else:
                response = getattr(module, validator['name'])(target_frame, validator['pattern']).validate()
            if response['result']:
                log.info(f"Validation for field [{key}] passed!")
            else:
                failed_validation.append({
                    'field': key,
                    'validator': validator,
                    'count': response['count']
                })
                print(failed_validation)
    if len(failed_validation) > 0:
        raise ValidationError(f"Failed to validate the following fields {failed_validation}. Failure Count is [{failed_validation}]")


class AbstractValidator:
    def validate(self, frame) -> dict: pass


class NricValidator(AbstractValidator):
    def validate(self, frame) -> dict:
        matched = self.frame[self.frame.str.match(r'(?i)^[STFG]\d{7}[A-Z]$')]
        if matched.size == self.frame.size:
            return {
                'result': True,
                'count': len(matched)
            }
        return {
            'result': False,
            'count': len(self.frame) - len(matched)
        }


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
    frame: pd.Series
    pattern: str

    def __init__(self, pattern):
        self.pattern = pattern
        super().__init__()

    def validate(self, frame) -> dict:
        matched = self.frame[self.frame.str.match(self.pattern)]
        if matched.size == self.frame.size:
            return {
                'result': True,
                'count': len(matched)
            }
        return {
            'result': False,
            'count': len(matched)
        }

class NanValidator(AbstractValidator):
    def validate(self, frame) -> dict:
        return super().validate(frame)


class RefValidator(AbstractValidator):
    frames: dict
    config: dict

    def __init__(self, config):
        self.config = config
        super().__init__()

    def validate(self, frame) -> dict:
        if self.config['type'] == "match":
            splits = self.config['ref'].split('.')
            target = self.frames[splits[0]][splits[1]]
            source = self.frames[self.config['segment']][self.config['key']]
            if len(target) > 1 and len(source) > 1:
                if target.equals(source):
                    return {
                        'result': True,
                        'count': target.value_counts().loc[True]
                    }
                return {
                    'result': False,
                    'count': target.value_counts().loc[False],
                }
            elif len(target) == 1 and len(source) > 1:
                data = source == target[0]
                if False not in data:
                    return {
                        'result': True,
                        'count': source.value_counts().loc[True]
                    }
                return {
                    'result': False,
                    'count': target.value_counts().loc[False],
                }
            elif len(target) > 1 and len(source) == 1:
                data = target == source[0]
                if False not in data:
                    return {
                        'result': True,
                        'count': target.sum().count()
                    }
                return {
                    'result': False,
                    'count': target[target == False].count()
                }
            else:
                if int(target[0]) == int(source[0]):
                    return {
                        'result': True,
                        'count': source.value_counts().loc[True]
                    }
                return {
                    'result': False,
                    'count': source.value_counts().loc[False],
                }
        if self.config['type'] == "count":
            target_count = len(self.frames[self.config['ref']])
            expected_count = self.frames[self.config['segment']]['recordCount'][0]
            if int(target_count) == int(expected_count):
                return {
                    'result': True,
                    'count': int(expected_count)
                }
            return {
                'result': False,
                'count': int(target_count)
            }
