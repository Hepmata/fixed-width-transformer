import pytest
from transformer import app_config


def test_converter_success_no_space():
    data = "1,2"
    expected_results = (1, 2)
    response = app_config.converter(data)
    assert expected_results == response


def test_converter_success_with_space():
    data = "1 , 2"
    expected_results = (1, 2)
    response = app_config.converter(data)
    assert expected_results == response


def test_converter_fail_empty():
    with pytest.raises(ValueError):
        app_config.converter('')


def test_converter_fail_none():
    with pytest.raises(ValueError):
        app_config.converter(None)
