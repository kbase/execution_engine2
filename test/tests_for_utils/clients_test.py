# This test only tests code that can be exercised without a network connection to services.
# That code is tested in integration tests.

from pytest import raises

from execution_engine2.utils.clients import UserClientSet
from utils_shared.test_utils import assert_exception_correct


def test_user_client_set_init_fail():
    user_client_set_init_fail(None, "foo", "bar", ValueError("cfg is required"))
    user_client_set_init_fail({}, "foo", "bar", ValueError("cfg is required"))
    user_client_set_init_fail(
        {"a": "b"}, None, "bar", ValueError("user_id is required")
    )
    user_client_set_init_fail(
        {"a": "b"}, "    \t  ", "bar", ValueError("user_id is required")
    )
    user_client_set_init_fail({"a": "b"}, "foo", None, ValueError("token is required"))
    user_client_set_init_fail(
        {"a": "b"}, "foo", "    \t   ", ValueError("token is required")
    )


def user_client_set_init_fail(cfg, user, token, expected):
    with raises(Exception) as e:
        UserClientSet(cfg, user, token)
    assert_exception_correct(e.value, expected)
