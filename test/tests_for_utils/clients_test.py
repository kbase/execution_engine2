# This test only tests code that can be exercised without a network connection to services.
# That code is tested in integration tests.

from pytest import raises
from unittest.mock import create_autospec

from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.utils.clients import UserClientSet, get_user_client_set, ClientSet
from utils_shared.test_utils import assert_exception_correct
from utils_shared.mock_utils import get_client_mocks, ALL_CLIENTS
from installed_clients.WorkspaceClient import Workspace

from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.utils.CatalogUtils import CatalogUtils
from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.job_requirements_resolver import JobRequirementsResolver
from execution_engine2.utils.KafkaUtils import KafkaClient
from execution_engine2.utils.SlackUtils import SlackClient

from installed_clients.authclient import KBaseAuth
from installed_clients.CatalogClient import Catalog


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


def test_client_set_init_fail():
    mocks = get_client_mocks(None, None, *ALL_CLIENTS)
    a = mocks[KBaseAuth]
    aa = mocks[AdminAuthUtil]
    c = mocks[Condor]
    ca = mocks[Catalog]
    j = mocks[JobRequirementsResolver]
    cu = mocks[CatalogUtils]
    k = mocks[KafkaClient]
    m = mocks[MongoUtil]
    s = mocks[SlackClient]
    n = None

    e = ValueError("auth cannot be a value that evaluates to false")
    _client_set_init_fail(n, aa, c, ca, j, cu, k, m, s, e)
    e = ValueError("auth_admin cannot be a value that evaluates to false")
    _client_set_init_fail(a, n, c, ca, j, cu, k, m, s, e)
    e = ValueError("condor cannot be a value that evaluates to false")
    _client_set_init_fail(a, aa, n, ca, j, cu, k, m, s, e)
    e = ValueError("catalog cannot be a value that evaluates to false")
    _client_set_init_fail(a, aa, c, n, j, cu, k, m, s, e)
    e = ValueError("requirements_resolver cannot be a value that evaluates to false")
    _client_set_init_fail(a, aa, c, ca, n, cu, k, m, s, e)
    e = ValueError("catalog_utils cannot be a value that evaluates to false")
    _client_set_init_fail(a, aa, c, ca, j, n, k, m, s, e)
    e = ValueError("kafka_client cannot be a value that evaluates to false")
    _client_set_init_fail(a, aa, c, ca, j, cu, n, m, s, e)
    e = ValueError("mongo_util cannot be a value that evaluates to false")
    _client_set_init_fail(a, aa, c, ca, j, cu, k, n, s, e)
    e = ValueError("slack_client cannot be a value that evaluates to false")
    _client_set_init_fail(a, aa, c, ca, j, cu, k, m, n, e)


def _client_set_init_fail(
    auth: KBaseAuth,
    auth_admin: AdminAuthUtil,
    condor: Condor,
    catalog: Catalog,
    requirements_resolver: JobRequirementsResolver,
    catalog_utils: CatalogUtils,
    kafka_client: KafkaClient,
    mongo_util: MongoUtil,
    slack_client: SlackClient,
    expected: Exception,
):
    with raises(Exception) as got:
        ClientSet(auth, auth_admin, condor, catalog, requirements_resolver, catalog_utils,
                  kafka_client, mongo_util, slack_client)
    assert_exception_correct(got.value, expected)
