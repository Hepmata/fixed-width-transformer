from dataclasses import dataclass


@dataclass()
class ConverterConfig:
    segment: str
    field_name: str
    name: str
