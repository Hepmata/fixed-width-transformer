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