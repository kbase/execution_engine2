"""
Integration tests that cover the entire codebase from API to database.

NOTE 1: These tests are designed to only be runnable after running docker-compose up

NOTE 2: These tests were set up quickly in order to debug a problem with administration related
calls. As such, the auth server was set up to run in test mode locally. If more integrations
are needed, they will need to be added either locally or as docker containers.
If the latter, the test auth and workspace integrations will likely need to be converted to
docker containers or exposed to other containers.

NOTE 3: Posting to Slack always fails silently.
"""

import os
import tempfile
import time

from configparser import ConfigParser
from threading import Thread
from pathlib import Path
import pymongo
from pytest import fixture
from typing import Dict
from tests_for_integration.auth_controller import AuthController
from tests_for_integration.workspace_controller import WorkspaceController
from utils_shared.test_utils import (
    get_full_test_config,
    get_ee2_test_config,
    EE2_CONFIG_SECTION,
    KB_DEPLOY_ENV,
    find_free_port,
    create_auth_login_token,
    create_auth_user,
    create_auth_role,
    set_custom_roles,
)
from execution_engine2.sdk.EE2Constants import ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
from installed_clients.execution_engine2Client import execution_engine2 as ee2client
from installed_clients.WorkspaceClient import Workspace

KEEP_TEMP_FILES = False
TEMP_DIR = Path("test_temp_can_delete")

# may need to make this configurable
JARS_DIR = Path("/opt/jars/lib/jars")

USER_READ_ADMIN = "readuser"
TOKEN_READ_ADMIN = None
USER_NO_ADMIN = "nouser"
TOKEN_NO_ADMIN = None
USER_WRITE_ADMIN = "writeuser"
TOKEN_WRITE_ADMIN = None


@fixture(scope="module")
def config() -> Dict[str, str]:
    yield get_ee2_test_config()


@fixture(scope="module")
def full_config() -> ConfigParser:
    yield get_full_test_config()


@fixture(scope="module")
def mongo_client(config):
    mc = pymongo.MongoClient(
        config["mongo-host"],
        username=config["mongo-user"],
        password=config["mongo-password"],
    )
    yield mc

    mc.close()


def _clean_db(mongo_client, db, db_user):
    try:
        mongo_client[db].command("dropUser", db_user)
    except pymongo.errors.OperationFailure as e:
        if f"User '{db_user}@{db}' not found" not in e.args[0]:
            raise  # otherwise ignore and continue, user is already toast
    mongo_client.drop_database(db)


def _create_db_user(mongo_client, db, db_user, password):
    mongo_client[db].command("createUser", db_user, pwd=password, roles=["readWrite"])


def _set_up_auth_users(auth_url):
    create_auth_role(auth_url, ADMIN_READ_ROLE, "ee2 admin read doohickey")
    create_auth_role(auth_url, ADMIN_WRITE_ROLE, "ee2 admin write thinger")

    global TOKEN_READ_ADMIN
    create_auth_user(auth_url, USER_READ_ADMIN, "display1")
    TOKEN_READ_ADMIN = create_auth_login_token(auth_url, USER_READ_ADMIN)
    set_custom_roles(auth_url, USER_READ_ADMIN, [ADMIN_READ_ROLE])

    global TOKEN_NO_ADMIN
    create_auth_user(auth_url, USER_NO_ADMIN, "display2")
    TOKEN_NO_ADMIN = create_auth_login_token(auth_url, USER_NO_ADMIN)

    global TOKEN_WRITE_ADMIN
    create_auth_user(auth_url, USER_WRITE_ADMIN, "display3")
    TOKEN_WRITE_ADMIN = create_auth_login_token(auth_url, USER_WRITE_ADMIN)
    set_custom_roles(auth_url, USER_WRITE_ADMIN, [ADMIN_WRITE_ROLE])


@fixture(scope="module")
def auth_url(config, mongo_client):
    auth_db = "api_to_db_auth_test"
    auth_mongo_user = "auth"
    # clean up from any previously failed test runs that left the db in place
    _clean_db(mongo_client, auth_db, auth_mongo_user)

    # make a user for the auth db
    _create_db_user(mongo_client, auth_db, auth_mongo_user, "authpwd")

    auth = AuthController(
        JARS_DIR,
        config["mongo-host"],
        auth_db,
        TEMP_DIR,
        mongo_user=auth_mongo_user,
        mongo_pwd="authpwd",
    )
    print(
        f"Started KBase Auth2 {auth.version} on port {auth.port} "
        + f"in dir {auth.temp_dir} in {auth.startup_count}s"
    )
    url = f"http://localhost:{auth.port}"

    _set_up_auth_users(url)

    yield url

    print(f"shutting down auth, KEEP_TEMP_FILES={KEEP_TEMP_FILES}")
    auth.destroy(not KEEP_TEMP_FILES)

    # Because the tests are run with mongo in a persistent docker container via docker-compose,
    # we need to clean up after ourselves.
    _clean_db(mongo_client, auth_db, auth_mongo_user)


@fixture(scope="module")
def ws_controller(config, mongo_client, auth_url):
    ws_db = "api_to_db_ws_test"
    ws_types_db = "api_to_db_ws_types_test"
    ws_mongo_user = "workspace"
    # clean up from any previously failed test runs that left the db in place
    _clean_db(mongo_client, ws_db, ws_mongo_user)
    _clean_db(mongo_client, ws_types_db, ws_mongo_user)

    # make a user for the ws dbs
    _create_db_user(mongo_client, ws_db, ws_mongo_user, "wspwd")
    _create_db_user(mongo_client, ws_types_db, ws_mongo_user, "wspwd")

    ws = WorkspaceController(
        JARS_DIR,
        config["mongo-host"],
        ws_db,
        ws_types_db,
        auth_url + "/testmode/",
        TEMP_DIR,
        mongo_user=ws_mongo_user,
        mongo_pwd="wspwd",
    )
    print(
        f"Started KBase Workspace {ws.version} on port {ws.port} "
        + f"in dir {ws.temp_dir} in {ws.startup_count}s"
    )
    yield ws

    print(f"shutting down workspace, KEEP_TEMP_FILES={KEEP_TEMP_FILES}")
    ws.destroy(not KEEP_TEMP_FILES)

    # Because the tests are run with mongo in a persistent docker container via docker-compose,
    # we need to clean up after ourselves.
    _clean_db(mongo_client, ws_db, ws_mongo_user)
    _clean_db(mongo_client, ws_types_db, ws_mongo_user)


def _update_config_and_create_config_file(full_config, auth_url, ws_controller):
    """
    Updates the config in place with the correct auth url for the tests and
    writes the updated config to a temporary file.

    Returns the file path.
    """
    # Don't call get_ee2_test_config here, we *want* to update the config object in place
    # so any other tests that use the config fixture run against the test auth server if they
    # access those keys
    ee2c = full_config[EE2_CONFIG_SECTION]
    ee2c["auth-service-url"] = auth_url + "/testmode/api/legacy/KBase/Sessions/Login"
    ee2c["auth-service-url-v2"] = auth_url + "/testmode/api/v2/token"
    ee2c["auth-url"] = auth_url + "/testmode"
    ee2c["auth-service-url-allow-insecure"] = "true"
    ee2c["workspace-url"] = f"http://localhost:{ws_controller.port}"

    deploy = tempfile.mkstemp(".cfg", "deploy-", dir=TEMP_DIR, text=True)
    os.close(deploy[0])

    with open(deploy[1], "w") as handle:
        full_config.write(handle)

    return deploy[1]


def _clear_dbs(
    mc: pymongo.MongoClient, config: Dict[str, str], ws_controller: WorkspaceController
):
    ee2 = mc[config["mongo-database"]]
    for name in ee2.list_collection_names():
        if not name.startswith("system."):
            # don't drop collection since that drops indexes
            ee2.get_collection(name).delete_many({})
    ws_controller.clear_db()


@fixture(scope="module")
def service(full_config, auth_url, mongo_client, config, ws_controller):
    # also updates the config in place so it contains the correct auth urls for any other
    # methods that use the config fixture
    cfgpath = _update_config_and_create_config_file(
        full_config, auth_url, ws_controller
    )
    print(f"created test deploy at {cfgpath}")
    _clear_dbs(mongo_client, config, ws_controller)

    prior_deploy = os.environ[KB_DEPLOY_ENV]
    # from this point on, calling the get_*_test_config methods will get the temp config file
    os.environ[KB_DEPLOY_ENV] = cfgpath
    # The server creates the configuration, impl, and application *AT IMPORT TIME* so we have to
    # import *after* setting the config path.
    # This is terrible design. Awful. It definitely wasn't me that wrote it over Xmas in 2012
    from execution_engine2 import execution_engine2Server

    portint = find_free_port()
    Thread(
        target=execution_engine2Server.start_server,
        kwargs={"port": portint},
        daemon=True,
    ).start()
    time.sleep(0.05)
    port = str(portint)
    print("running ee2 service at localhost:" + port)
    yield port

    # shutdown the server
    # SampleServiceServer.stop_server()  <-- this causes an error.
    # See the server file for the full scoop, but in short, the stop method expects a _proc
    # package variable to be set, but start doesn't always set it, and that causes an error.

    # Tests are run in the same process so we need to be put the environment back the way it was
    os.environ[KB_DEPLOY_ENV] = prior_deploy

    if not KEEP_TEMP_FILES:
        os.remove(cfgpath)


@fixture
def ee2_port(service, mongo_client, config, ws_controller):
    _clear_dbs(mongo_client, config, ws_controller)

    yield service


def test_is_admin_success(ee2_port):
    ee2cli_read = ee2client("http://localhost:" + ee2_port, token=TOKEN_READ_ADMIN)
    ee2cli_no = ee2client("http://localhost:" + ee2_port, token=TOKEN_NO_ADMIN)
    ee2cli_write = ee2client("http://localhost:" + ee2_port, token=TOKEN_WRITE_ADMIN)

    # note that if we ever need to have Java talk to ee2 these responses will break the SDK client
    assert ee2cli_read.is_admin() is True
    assert ee2cli_no.is_admin() is False
    assert ee2cli_write.is_admin() is True


def test_get_admin_permission_success(ee2_port):
    ee2cli_read = ee2client("http://localhost:" + ee2_port, token=TOKEN_READ_ADMIN)
    ee2cli_no = ee2client("http://localhost:" + ee2_port, token=TOKEN_NO_ADMIN)
    ee2cli_write = ee2client("http://localhost:" + ee2_port, token=TOKEN_WRITE_ADMIN)

    assert ee2cli_read.get_admin_permission() == {"permission": "r"}
    assert ee2cli_no.get_admin_permission() == {"permission": "n"}
    assert ee2cli_write.get_admin_permission() == {"permission": "w"}


def test_temporary_check_ws(ee2_port, ws_controller):
    wsc = Workspace(ws_controller.get_url(), token=TOKEN_NO_ADMIN)
    ws = wsc.create_workspace({"workspace": "foo"})
    assert ws[1] == "foo"
