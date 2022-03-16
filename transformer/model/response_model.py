import dataclasses
import typing
from dataclasses import dataclass
import json


@dataclass
class ExecutorResponse:
    sourceBucket: str
    sourceFile: str
    requestTime: str
    requestId: str
    statusCode: int

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))


@dataclass
class SuccessResultResponse(ExecutorResponse):
    resultTopic: str
    recordCount: int
    validations: typing.List
    constraints: typing.List
    pass


@dataclass
class ValidationErrorResponse(ExecutorResponse):
    recordCount: int
    failureReason: str
    validations: typing.List


@dataclass
class ConstraintErrorResponse(ExecutorResponse):
    recordCount: int
    failureReason: str
    failureMessage: str
    constraints: typing.List


@dataclass
class GenericErrorResponse(ExecutorResponse):
    failureReason: str
    failureMessage: str
