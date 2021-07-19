from transformer import validator
from transformer.library.exceptions import ValidationError

class PreValidate(object):
    def __init__(self, run) -> None:
        self.run = run

    def __call__(self, *args, **kwargs):
        cfg = args[0]
        file_name = args[1]
        print("----Running PreValidations----")
        results = {cfg.segment: self.run(self, cfg, file_name)}
        failed_validations = []
        for val_config in cfg.validations:
            try:
                getattr(validator, val_config.validator_name)(val_config).validate(results)
            except ValidationError as e:
                failed_validations.append(e)

        if len(failed_validations) > 0:
            raise ValidationError(f"Failed validations for {failed_validations}", cfg.segment, "ALL", 0, 0)
        return results[cfg.segment]


class PostValidate(object):
    def __init__(self, function):
        self.function = function

    def __call__(self, *args, **kwargs):
        self.function()