# This test only tests code that can be exercised without a network connection to services.
# That code is tested in integration tests.

from pytest import raises
from unittest.mock import create_autospec

from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.utils.clients import UserClientSet, get_user_client_set
from utils_shared.test_utils import assert_exception_correct
from installed_clients.WorkspaceClient import Workspace


def test_get_user_client_set_fail():
    ws_err = "missing workspace-url in configuration"
    get_user_client_set_fail(None, "foo", "bar", ValueError("cfg is required"))
    get_user_client_set_fail({}, "foo", "bar", ValueError("cfg is required"))
    get_user_client_set_fail({"a": "b"}, "foo", "bar", ValueError(ws_err))
    get_user_client_set_fail({"workspace-url": None}, "foo", "bar", ValueError(ws_err))
    get_user_client_set_fail(
        {"workspace-url": "   \t    "}, "foo", "bar", ValueError(ws_err)
    )
    get_user_client_set_fail(
        {"workspace-url": "https://ws.com"},
        None,
        "bar",
        ValueError("user_id is required"),
    )
    get_user_client_set_fail(
        {"workspace-url": "https://ws.com"},
        "    \t  ",
        "bar",
        ValueError("user_id is required"),
    )
    get_user_client_set_fail(
        {"workspace-url": "https://ws.com"},
        "foo",
        None,
        ValueError("token is required"),
    )
    get_user_client_set_fail(
        {"workspace-url": "https://ws.com"},
        "foo",
        "    \t   ",
        ValueError("token is required"),
    )


def get_user_client_set_fail(cfg, user, token, expected):
    with raises(Exception) as e:
        get_user_client_set(cfg, user, token)
    assert_exception_correct(e.value, expected)


def test_user_client_set_init_fail():
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    wsa = WorkspaceAuth("u", ws)
    user_client_set_init_fail(None, "t", ws, wsa, ValueError("user_id is required"))
    user_client_set_init_fail("  \t ", "t", ws, wsa, ValueError("user_id is required"))
    user_client_set_init_fail("u", None, ws, wsa, ValueError("token is required"))
    user_client_set_init_fail("u", "   \t   ", ws, wsa, ValueError("token is required"))
    user_client_set_init_fail("u", "t", None, wsa, ValueError("workspace is required"))
    user_client_set_init_fail(
        "u", "t", ws, None, ValueError("workspace_auth is required")
    )


def user_client_set_init_fail(user, token, ws_client, ws_auth, expected):
    with raises(Exception) as e:
        UserClientSet(user, token, ws_client, ws_auth)
    assert_exception_correct(e.value, expected)
