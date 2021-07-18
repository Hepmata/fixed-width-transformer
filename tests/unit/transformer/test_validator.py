import uuid
from unittest.mock import MagicMock

import numpy as np
import pytest
from transformer.config import ValidatorConfig
from transformer.library.exceptions import ValidationError
import pandas as pd
from transformer import validator
# import pytest
# 
# from transformer import validator
# import pandas as pd
# 
# class TestNricValidator:
#     def test_nric_validator_success(self):
#         nric = "S1234567A"
#         nric = pd.DataFrame({"nric": []})
#         response = validator.NricValidator().validate(nric)
#         assert response
# 
#     def test_nric_validator_fail_empty(self):
#         nric = ""
#         response = validators.nric_validator(nric)
#         assert not response
# 
#     def test_nric_validator_fail_null(self):
#         with pytest.raises(TypeError):
#             validators.nric_validator(None)
# 
#     def test_nric_validator_fail_invalid_nric(self):
#         nric = "S123456"
#         response = validators.nric_validator(nric)
#         assert not response
# 
# 
# class TestRegexValidator:
#     def test_regex_validator_success(self):
#         pattern = r'^SimpleTest\d+$'
#         content = "SimpleTest400"
#         response = validators.regex_validator(content, pattern)
# 
#         assert response
# 
#     def test_regex_validator_fail_invalid_regex(self):
#         pattern = "asd"
#         content = "SimpleTest400"
#         response = validators.regex_validator(content, pattern)
# 
#         assert not response
# 
#     def test_regex_validator_fail_empty_content(self):
#         pattern = r'^SimpleTest\d+$'
#         content = ""
#         response = validators.regex_validator(content, pattern)
# 
#         assert not response
# 
#     def test_regex_validator_fail_null_content(self):
#         pattern = r'^SimpleTest\d+$'
#         content = None
#         with pytest.raises(TypeError):
#             validators.regex_validator(content, pattern)
# 
#     def test_regex_validator_fail_null_pattern(self):
#         pattern = None
#         content = "asd"
#         with pytest.raises(TypeError):
#             validators.regex_validator(content, pattern)
# 
#     def test_regex_validator_fail_null_both(self):
#         with pytest.raises(TypeError):
#             validators.regex_validator(None, None)

class TestNricValidator:
    @pytest.fixture
    def dataframes(self):
        dfs = {
            "header": pd.DataFrame({
                "field1": ["One"],
                "field2": uuid.uuid4().__str__()
            }),
            "body": pd.DataFrame({
                "body1": ["One", "Two", "Three", "Four", "Five"],
                "body2": [1, 2, 3, 4, 5],
                "ic": ["S1234567A", "S1234567A", "S1234567A", "S1234567A", "S1234567A"]
            }),
            "footer": pd.DataFrame({
                "recordCount": [5]
            })
        }
        return dfs

    def test_success(self, dataframes):
        config = ValidatorConfig(
            "body",
            "ic"
        )
        validator.NricValidator(config).validate(dataframes)

    def test_failure(self):
        dfs = {
            "body": pd.DataFrame({
                "ic": ["S1234567A", "00000"]
            })
        }
        config = ValidatorConfig(
            "body",
            "ic"
        )
        with pytest.raises(ValidationError):
            validator.NricValidator(config).validate(dfs)


class TestNanValidator:

    @pytest.fixture
    def dataframes(self):
        return {
            "body": pd.DataFrame({
                "field": ["1", "asdasd", "asdasd"]
            })
        }

    def test_success_all(self, dataframes):
        config = ValidatorConfig(
            "body",
            "ALL"
        )
        validator.NanValidator(config).validate(dataframes)

    def test_success_field(self, dataframes):
        config = ValidatorConfig(
            "body",
            "field"
        )
        validator.NanValidator(config).validate(dataframes)

    def test_failure_all(self, dataframes):
        dataframes['body'] = pd.DataFrame({
            "field": ["1", np.NAN, "asdasd"],
            "field2": [1, 2, 3]
        })
        config = ValidatorConfig(
            "body",
            "ALL"
        )
        with pytest.raises(ValidationError):
            validator.NanValidator(config).validate(dataframes)

    def test_failure_field(self, dataframes):
        dataframes['body'] = pd.DataFrame({
            "field": ["1", np.NAN, "asdasd"],
            "field2": [1, 2, 3]
        })
        config = ValidatorConfig(
            "body",
            "field"
        )
        with pytest.raises(ValidationError):
            validator.NanValidator(config).validate(dataframes)