from dataclasses import dataclass


@dataclass()
class ConverterConfig:
    segment: str
    fieldName: str
    name: str
