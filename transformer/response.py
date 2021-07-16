import dataclasses


@dataclasses.dataclass
class ResultResponse:
    destination: dict
    def __dict__(self):
        pass

@dataclasses.dataclass
class ErrorResponse:
    pass