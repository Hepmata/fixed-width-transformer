import uuid

import numpy as np
import pytest
from transformer.config import ValidatorConfig
from transformer.library.exceptions import ValidationError, MissingConfigError
import pandas as pd
from transformer import validator


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

    class TestSuccess:
        def test_nric(self, dataframes):
            config = ValidatorConfig(
                validator_name="",
                segment="body",
                field_name="ic"
            )
            validator.NricValidator(config).validate(dataframes)

    class TestFailure:
        def test_invalid_nric(self):
            dfs = {
                "body": pd.DataFrame({
                    "ic": ["S1234567A", "00000"]
                })
            }
            config = ValidatorConfig(
                validator_name="",
                segment="body",
                field_name="ic"
            )
            with pytest.raises(ValidationError):
                validator.NricValidator(config).validate(dfs)


class TestNaNValidator:
    @pytest.fixture
    def dataframes(self):
        return {
            "body": pd.DataFrame({
                "field": ["1", "asdasd", "asdasd"]
            })
        }

    class TestSuccess:
        def test_all(self, dataframes):
            config = ValidatorConfig(
                validator_name="",
                segment="body",
                field_name="ALL"
            )
            validator.NaNValidator(config).validate(dataframes)

        def test_field(self, dataframes):
            config = ValidatorConfig(
                validator_name="",
                segment="body",
                field_name="field"
            )
            validator.NaNValidator(config).validate(dataframes)

    class TestFailure:
        def test_failure_all(self, dataframes):
            dataframes['body'] = pd.DataFrame({
                "field": ["1", np.NAN, "asdasd"],
                "field2": [1, 2, 3]
            })
            config = ValidatorConfig(
                validator_name="",
                segment="body",
                field_name="ALL"
            )
            with pytest.raises(ValidationError):
                validator.NaNValidator(config).validate(dataframes)

        def test_failure_field(self, dataframes):
            dataframes['body'] = pd.DataFrame({
                "field": ["1", np.NAN, "asdasd"],
                "field2": [1, 2, 3]
            })
            config = ValidatorConfig(
                validator_name="",
                segment="body",
                field_name="field"
            )
            with pytest.raises(ValidationError):
                validator.NaNValidator(config).validate(dataframes)


class TestRefValidator:
    class TestSuccess:
        def test_count(self):
            dataframes = {
                "body": pd.DataFrame({
                    "field1": [1,2,3,4,5]
                }),
                "footer": pd.DataFrame({
                    "recordCount": [5]
                })
            }
            config = ValidatorConfig(
                validator_name="",
                segment="body",
                field_name="field1",
                arguments={
                    "type": "count",
                    "ref": "footer.recordCount"
                }
            )
            validator.RefValidator(config).validate(dataframes)

        def test_count_reverse(self):
            dataframes = {
                "body": pd.DataFrame({
                    "field1": [1,2,3,4,5]
                }),
                "footer": pd.DataFrame({
                    "recordCount": [5]
                })
            }
            config = ValidatorConfig(
                validator_name="",
                segment="footer",
                field_name="recordCount",
                arguments={
                    "type": "count",
                    "ref": "body.field1"
                }
            )
            validator.RefValidator(config).validate(dataframes)

        def test_match(self):
            dataframes = {
                "body": pd.DataFrame({
                    "field1": [1,2,3,4,5],
                    "matcherField": [1,2,3,4,5]
                }),
                "footer": pd.DataFrame({
                    "recordCount": [5]
                })
            }
            config = ValidatorConfig(
                validator_name="",
                segment="body",
                field_name="field1",
                arguments={
                    "type": "match",
                    "ref": "body.matcherField"
                }
            )
            validator.RefValidator(config).validate(dataframes)

        def test_match_reverse(self):
            dataframes = {
                "body": pd.DataFrame({
                    "field1": [1, 2, 3, 4, 5],
                }),
                "footer": pd.DataFrame({
                    "recordCount": [5]
                }),
                "match": pd.DataFrame({
                    "field1": [1, 2, 3, 4, 5]
                })
            }
            config = ValidatorConfig(
                validator_name="",
                segment="body",
                field_name="field1",
                arguments={
                    "type": "match",
                    "ref": "match.field1"
                }
            )
            validator.RefValidator(config).validate(dataframes)

    class TestFailure:
        def test_failure(self):
            dataframes = {
                "body": pd.DataFrame({
                    "field1": [1, 2, 3, 4, 5]
                }),
                "footer": pd.DataFrame({
                    "recordCount": [2]
                })
            }
            config = ValidatorConfig(
                validator_name="",
                segment="footer",
                field_name="recordCount",
                arguments={
                    "type": "count",
                    "ref": "body.field1"
                }
            )
            with pytest.raises(ValidationError):
                validator.RefValidator(config).validate(dataframes)


class TestRegexValidator:
    @pytest.fixture
    def dataframes(self):
        return {
            "footer": pd.DataFrame({
                "field1": ["SX1", "SX2", "SX3", "SX4", "SX5"]
            })
        }

    class TestSuccess:
        def test_regex(self, dataframes):
            config = ValidatorConfig(
                validator_name="",
                segment="footer",
                field_name="field1",
                arguments={
                    "pattern": r'^SX\d+$'
                }
            )
            validator.RegexValidator(config).validate(dataframes)

    class TestFailure:
        def test_invalid_pattern(self, dataframes):
            config = ValidatorConfig(
                validator_name="",
                segment="footer",
                field_name="field1",
                arguments={
                    "pattern": "asdasda"
                }
            )
            with pytest.raises(ValidationError):
                validator.RegexValidator(config).validate(dataframes)

        def test_missing_argument(self, dataframes):
            config = ValidatorConfig(
                validator_name="",
                segment="footer",
                field_name="field1",
            )
            with pytest.raises(MissingConfigError):
                validator.RegexValidator(config).validate(dataframes)

        def test_invalid_argument_type(self, dataframes):
            config = ValidatorConfig(
                validator_name="",
                segment="footer",
                field_name="field1",
                arguments={
                    "pattern": 0
                }
            )
            with pytest.raises(MissingConfigError):
                validator.RegexValidator(config).validate(dataframes)

        def test_mismatch(self, dataframes):
            config = ValidatorConfig(
                validator_name="",
                segment="footer",
                field_name="field1",
                arguments={
                    "pattern": r'^SX$'
                }
            )
            with pytest.raises(ValidationError):
                validator.RegexValidator(config).validate(dataframes)