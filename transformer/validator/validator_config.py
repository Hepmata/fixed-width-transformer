from dataclasses import dataclass


@dataclass
class ValidatorFieldConfig:
    name: str
    arguments: dict


@dataclass
class ValidatorConfig:
    segment: str
    field_name: str
    validators: [ValidatorFieldConfig]

