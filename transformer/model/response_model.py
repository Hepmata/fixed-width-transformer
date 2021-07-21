from dataclasses import dataclass


@dataclass
class ResultResponse:
    destination: dict
    pass


@dataclass
class ErrorResponse:
    pass
