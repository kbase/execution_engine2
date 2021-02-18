"""
Integration tests that cover the entire codebase from API to database.

NOTE 1: These tests are designed to only be runnable after running docker-compose up

NOTE 2: These tests were set up quickly in order to debug a problem with administration related
calls. As such, the auth server was set up to run in test mode locally. If more integrations
(e.g. the workspace) are needed, they will need to be added either locally or as docker containers.
If the latter, the test auth integration will likely need to be converted to a docker container or
exposed to other containers.
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

KEEP_TEMP_FILES = True
AUTH_DB = "api_to_db_test"
AUTH_MONGO_USER = "auth"
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


def _clean_auth_db(mongo_client):
    try:
        mongo_client[AUTH_DB].command("dropUser", AUTH_MONGO_USER)
    except pymongo.errors.OperationFailure as e:
        if f"User '{AUTH_MONGO_USER}@{AUTH_DB}' not found" not in e.args[0]:
            raise  # otherwise ignore and continue, user is already toast
    mongo_client.drop_database(AUTH_DB)


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
    # clean up from any previously failed test runs that left the db in place
    _clean_auth_db(mongo_client)

    # make a user for the auth db
    mongo_client[AUTH_DB].command(
        "createUser", AUTH_MONGO_USER, pwd="authpwd", roles=["readWrite"]
    )
    auth = AuthController(
        JARS_DIR,
        config["mongo-host"],
        AUTH_DB,
        TEMP_DIR,
        mongo_user=AUTH_MONGO_USER,
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
    _clean_auth_db(mongo_client)


def _update_config_and_create_config_file(full_config, auth_url):
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

    deploy = tempfile.mkstemp(".cfg", "deploy-", dir=TEMP_DIR, text=True)
    os.close(deploy[0])

    with open(deploy[1], "w") as handle:
        full_config.write(handle)

    return deploy[1]


def _clear_ee2_db(mc: pymongo.MongoClient, config: Dict[str, str]):
    ee2 = mc[config["mongo-database"]]
    for name in ee2.list_collection_names():
        if not name.startswith("system."):
            # don't drop collection since that drops indexes
            ee2.get_collection(name).delete_many({})


@fixture(scope="module")
def service(full_config, auth_url, mongo_client, config):
    # also updates the config in place so it contains the correct auth urls for any other
    # methods that use the config fixture
    cfgpath = _update_config_and_create_config_file(full_config, auth_url)
    _clear_ee2_db(mongo_client, config)

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

    if not KEEP_TEMP_FILES:
        os.remove(cfgpath)


@fixture
def ee2_port(service, mongo_client, config):
    _clear_ee2_db(mongo_client, config)

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

    assert ee2cli_read.get_admin_permission() == {'permission': 'r'}
    assert ee2cli_no.get_admin_permission() == {'permission': 'n'}
    assert ee2cli_write.get_admin_permission() == {'permission': 'w'}
