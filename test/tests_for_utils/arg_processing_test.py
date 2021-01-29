from pytest import raises

from execution_engine2.utils.arg_processing import parse_bool
from utils_shared.test_utils import assert_exception_correct


def test_parse_bool_success():
    testcases = {
        None: False,
        True: True,
        False: False,
        -1: True,
        1: True,
        0: False,
        100: True,
        -100: True,
        "True": True,
        "TRUE": True,
        "true": True,
        "False": False,
        "FALSE": False,
        "false": False,
    }

    for arg, expected in testcases.items():
        assert parse_bool(arg) is expected, f"Testcase: {arg}"


def test_parse_bool_failure():
    testcases = ["Truthy", "fawlse", " "]

    for tc in testcases:
        with raises(Exception) as e:
            parse_bool(tc)
        assert_exception_correct(e.value, ValueError(f"{tc} is not a boolean value"))
