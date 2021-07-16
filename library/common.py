import os


def check_environment_variables(variables: list):
    error_list = []
    data = []
    for variable in variables:
        try:
            data.append(os.environ[variable])
        except KeyError as e:
            error_list.append(variable)
    if len(error_list) > 0:
        raise KeyError(f"Environment variables {error_list} are missing")
    return data
