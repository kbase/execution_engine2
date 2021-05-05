"""
Integration tests that cover the entire codebase from API to database.

NOTE 1: These tests are designed to only be runnable after running docker-compose up.

NOTE 2: These tests were set up quickly in order to debug a problem with administration related
calls. As such, the auth server was set up to run in test mode locally. If more integrations
are needed, they will need to be added either locally or as docker containers.
If the latter, the test auth and workspace integrations will likely need to be converted to
docker containers or exposed to other containers.

NOTE 3: Although this is supposed to be an integration test, the catalog service and htcondor
are still mocked out as bringing them up would take a large amount of effort. Someday...

NOTE 4: Kafka notes
    a) Currently nothing listens to the kafka feed.
    b) When running the tests, the kafka producer logs that kafka cannot be reached. However,
        this error is silent otherwise.
    c) I wasn't able to contact the docker kafka service with the kafka-python client either.
    d) As such, Kafka is not tested. Once tests are added, at least one test should check that
        something sensible happens if a kafka message cannot be sent.

NOTE 5: EE2 posting to Slack always fails silently in tests. Currently slack calls are not tested.
"""

# TODO add more integration tests, these are not necessarily exhaustive

import os
import tempfile
import time
import htcondor

from bson import ObjectId
from configparser import ConfigParser
from threading import Thread
from pathlib import Path
import pymongo
from pytest import fixture, raises
from typing import Dict
from unittest.mock import patch, create_autospec, ANY, call

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
    assert_close_to_now,
    assert_exception_correct,
)
from execution_engine2.sdk.EE2Constants import ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
from installed_clients.baseclient import ServerError
from installed_clients.execution_engine2Client import execution_engine2 as ee2client
from installed_clients.WorkspaceClient import Workspace

# in the future remove this
from tests_for_utils.Condor_test import _get_common_sub

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

USER_KBASE_CONCIERGE = "kbaseconcierge"
TOKEN_KBASE_CONCIERGE = None

USER_WS_READ_ADMIN = "wsreadadmin"
TOKEN_WS_READ_ADMIN = None
USER_WS_FULL_ADMIN = "wsfulladmin"
TOKEN_WS_FULL_ADMIN = None
WS_READ_ADMIN = "WS_READ_ADMIN"
WS_FULL_ADMIN = "WS_FULL_ADMIN"

CAT_GET_MODULE_VERSION = "installed_clients.CatalogClient.Catalog.get_module_version"
CAT_LIST_CLIENT_GROUPS = (
    "installed_clients.CatalogClient.Catalog.list_client_group_configs"
)

# from test/deploy.cfg
MONGO_EE2_DB = "ee2"
MONGO_EE2_JOBS_COL = "ee2_jobs"

_MOD = "mod.meth"
_APP = "mod/app"


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


def _set_up_auth_user(auth_url, user, display, roles=None):
    create_auth_user(auth_url, user, display)
    if roles:
        set_custom_roles(auth_url, user, roles)
    return create_auth_login_token(auth_url, user)


def _set_up_auth_users(auth_url):
    create_auth_role(auth_url, ADMIN_READ_ROLE, "ee2 admin read doohickey")
    create_auth_role(auth_url, ADMIN_WRITE_ROLE, "ee2 admin write thinger")
    create_auth_role(auth_url, WS_READ_ADMIN, "wsr")
    create_auth_role(auth_url, WS_FULL_ADMIN, "wsf")

    global TOKEN_READ_ADMIN
    TOKEN_READ_ADMIN = _set_up_auth_user(
        auth_url, USER_READ_ADMIN, "display1", [ADMIN_READ_ROLE]
    )

    global TOKEN_NO_ADMIN
    TOKEN_NO_ADMIN = _set_up_auth_user(auth_url, USER_NO_ADMIN, "display2")

    global TOKEN_WRITE_ADMIN
    TOKEN_WRITE_ADMIN = _set_up_auth_user(
        auth_url, USER_WRITE_ADMIN, "display3", [ADMIN_WRITE_ROLE]
    )

    global TOKEN_KBASE_CONCIERGE
    TOKEN_KBASE_CONCIERGE = _set_up_auth_user(
        auth_url, USER_KBASE_CONCIERGE, "concierge"
    )

    global TOKEN_WS_READ_ADMIN
    TOKEN_WS_READ_ADMIN = _set_up_auth_user(
        auth_url, USER_WS_READ_ADMIN, "wsra", [WS_READ_ADMIN]
    )

    global TOKEN_WS_FULL_ADMIN
    TOKEN_WS_FULL_ADMIN = _set_up_auth_user(
        auth_url, USER_WS_FULL_ADMIN, "wsrf", [WS_FULL_ADMIN]
    )


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


def _add_ws_types(ws_controller):
    wsc = Workspace(f"http://localhost:{ws_controller.port}", token=TOKEN_WS_FULL_ADMIN)
    wsc.request_module_ownership("Trivial")
    wsc.administer({"command": "approveModRequest", "module": "Trivial"})
    wsc.register_typespec(
        {
            "spec": """
                module Trivial {
                    /* @optional dontusethisfieldorifyoudomakesureitsastring */
                    typedef structure {
                        string dontusethisfieldorifyoudomakesureitsastring;
                    } Object;
                };
                """,
            "dryrun": 0,
            "new_types": ["Object"],
        }
    )
    wsc.release_module("Trivial")


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
    _add_ws_types(ws)

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


######## run_job tests ########


def _get_htc_mocks():
    sub = create_autospec(htcondor.Submit, spec_set=True, instance=True)
    schedd = create_autospec(htcondor.Schedd, spec_set=True, instance=True)
    txn = create_autospec(htcondor.Transaction, spec_set=True, instance=True)
    return sub, schedd, txn


def _finish_htc_mocks(sub_init, schedd_init, sub, schedd, txn):
    sub_init.return_value = sub
    schedd_init.return_value = schedd
    # mock context manager ops
    schedd.transaction.return_value = txn
    txn.__enter__.return_value = txn
    return sub, schedd, txn


def _check_htc_calls(sub_init, sub, schedd_init, schedd, txn, expected_sub):
    sub_init.assert_called_once_with(expected_sub)
    schedd_init.assert_called_once_with()
    schedd.transaction.assert_called_once_with()
    sub.queue.assert_called_once_with(txn, 1)


def _set_up_workspace_objects(ws_controller, token, ws_name="foo"):
    wsc = Workspace(ws_controller.get_url(), token=token)
    wsid = wsc.create_workspace({"workspace": ws_name})[0]
    wsc.save_objects(
        {
            "id": wsid,
            "objects": [
                {"name": "one", "type": "Trivial.Object-1.0", "data": {}},
                {"name": "two", "type": "Trivial.Object-1.0", "data": {}},
            ],
        }
    )


def _get_run_job_param_set(app_id=_APP, job_reqs=None, as_admin=False):
    return {
        "method": _MOD,
        "app_id": app_id,
        "wsid": 1,
        "source_ws_objects": ["1/1/1", "1/2/1"],
        "params": [{"foo": "bar"}, 42],
        "service_ver": "beta",
        "parent_job_id": "totallywrongid",
        "job_requirements": job_reqs,
        "as_admin": as_admin,
        "meta": {
            "run_id": "rid",
            "token_id": "tid",
            "tag": "yourit",
            "cell_id": "cid",
            "thiskey": "getssilentlydropped",
        },
    }


def _get_condor_sub_for_rj_param_set(
    job_id,
    user,
    token,
    clientgroup,
    cpu,
    mem,
    disk,
    parent_job_id="totallywrongid",
    app_id=_APP,
    app_module="mod",
):
    expected_sub = _get_common_sub(job_id)
    expected_sub.update(
        {
            "JobBatchName": job_id,
            "arguments": f"{job_id} https://ci.kbase.us/services/ee2",
            "+KB_PARENT_JOB_ID": f'"{parent_job_id}"',
            "+KB_MODULE_NAME": '"mod"',
            "+KB_FUNCTION_NAME": '"meth"',
            "+KB_APP_ID": f'"{app_id}"' if app_id else "",
            "+KB_APP_MODULE_NAME": f'"{app_module}"' if app_module else "",
            "+KB_WSID": '"1"',
            "+KB_SOURCE_WS_OBJECTS": '"1/1/1,1/2/1"',
            "request_cpus": f"{cpu}",
            "request_memory": f"{mem}MB",
            "request_disk": f"{disk}GB",
            "requirements": f'regexp("{clientgroup}",CLIENTGROUP)',
            "+KB_CLIENTGROUP": f'"{clientgroup}"',
            "Concurrency_Limits": f"{user}",
            "+AccountingGroup": f'"{user}"',
            "environment": (
                '"DOCKER_JOB_TIMEOUT=604805 KB_ADMIN_AUTH_TOKEN=test_auth_token '
                + f"KB_AUTH_TOKEN={token} CLIENTGROUP={clientgroup} JOB_ID={job_id} "
                + "CONDOR_ID=$(Cluster).$(Process) PYTHON_EXECUTABLE=/miniconda/bin/python "
                + f'DEBUG_MODE=False PARENT_JOB_ID={parent_job_id} "'
            ),
            "leavejobinqueue": "true",
            "initial_dir": "../scripts/",
            "+Owner": '"condor_pool"',
            "executable": "../scripts//../scripts/execute_runner.sh",
            "transfer_input_files": "../scripts/JobRunner.tgz",
        }
    )
    return expected_sub


def _get_mongo_job(mongo_client, job_id, has_queued=True):
    # also checks and removes the queued and updated times
    job = mongo_client[MONGO_EE2_DB][MONGO_EE2_JOBS_COL].find_one(
        {"_id": ObjectId(job_id)}
    )
    assert_close_to_now(job.pop("updated"))
    if has_queued:
        assert_close_to_now(job.pop("queued"))
    return job


def _check_mongo_job(
    mongo_client, job_id, user, app_id, clientgroup, cpu, mem, disk, githash
):
    job = _get_mongo_job(mongo_client, job_id)
    expected_job = {
        "_id": ObjectId(job_id),
        "user": user,
        "authstrat": "kbaseworkspace",
        "wsid": 1,
        "status": "queued",
        "job_input": {
            "wsid": 1,
            "method": _MOD,
            "params": [{"foo": "bar"}, 42],
            "service_ver": githash,
            "source_ws_objects": ["1/1/1", "1/2/1"],
            "parent_job_id": "totallywrongid",
            "requirements": {
                "clientgroup": clientgroup,
                "cpu": cpu,
                "memory": mem,
                "disk": disk,
            },
            "narrative_cell_info": {
                "run_id": "rid",
                "token_id": "tid",
                "tag": "yourit",
                "cell_id": "cid",
            },
        },
        "child_jobs": [],
        "batch_job": False,
        "scheduler_id": "123",
        "scheduler_type": "condor",
    }
    if app_id:
        expected_job["job_input"]["app_id"] = app_id
    assert job == expected_job


def test_run_job_no_app_id(ee2_port, ws_controller, mongo_client):
    _run_job(
        ee2_port,
        ws_controller,
        mongo_client,
        catalog_return=[{"client_groups": ['{"request_cpus":8,"request_memory":5}']}],
    )


def test_run_job_with_app_id(ee2_port, ws_controller, mongo_client):
    _run_job(
        ee2_port,
        ws_controller,
        mongo_client,
        app_id="mod/app",
        app_mod="mod",
        catalog_return=[{"client_groups": ['{"request_cpus":8,"request_memory":5}']}],
    )


def test_run_job_with_job_requirements_full(ee2_port, ws_controller, mongo_client):
    """
    Tests running a job where all requirements are specified on input.
    """

    def modify_sub(sub):
        del sub["Concurrency_Limits"]
        sub["requirements"] = (
            '(CLIENTGROUP == "extreme") && (after == "pantsremoval") && '
            + '(beforemy == "2pmsalonappt")'
        )
        sub["+AccountingGroup"] = '"borishesgoodforit"'
        sub["environment"] = sub["environment"].replace(
            "DEBUG_MODE=False", "DEBUG_MODE=True"
        )

    _run_job(
        ee2_port,
        ws_controller,
        mongo_client,
        job_reqs={
            "request_cpus": 21,
            "request_memory": 34,
            "request_disk": 99,
            "client_group": "extreme",
            "client_group_regex": 0,
            "bill_to_user": "borishesgoodforit",
            "ignore_concurrency_limits": "true",
            "scheduler_requirements": {
                "beforemy": "2pmsalonappt",
                "after": "pantsremoval",
            },
            "debug_mode": True,
        },
        modify_sub=modify_sub,
        clientgroup="extreme",
        cpu=21,
        mem=34,
        disk=99,
        catalog_return=[
            {
                "client_groups": [
                    '{"client_group":"njs","request_cpus":8,"request_memory":5}'
                ]
            }
        ],
        as_admin=7,  # truthy
        user=USER_WRITE_ADMIN,
        token=TOKEN_WRITE_ADMIN,
    )


def test_run_job_with_job_requirements_mixed(ee2_port, ws_controller, mongo_client):
    """
    Tests running a job where requirements are specified on input, from the catalog, and from
    the deploy.cfg file.
    """
    _run_job(
        ee2_port,
        ws_controller,
        mongo_client,
        job_reqs={"request_cpus": 9},
        clientgroup="njs",
        cpu=9,
        mem=5,
        disk=30,
        catalog_return=[{"client_groups": ['{"request_cpus":8,"request_memory":5}']}],
        as_admin="wheee",  # truthy
        user=USER_WRITE_ADMIN,
        token=TOKEN_WRITE_ADMIN,
    )


def _run_job(
    ee2_port,
    ws_controller,
    mongo_client,
    app_id=None,
    app_mod=None,
    job_reqs=None,
    modify_sub=lambda x: x,
    clientgroup="njs",
    cpu=8,
    mem=5,
    disk=30,
    catalog_return=None,
    as_admin=False,
    user=None,
    token=None,
):
    # values in the method sig are set at the time of method creation, at which time the
    # user and token fields haven't yet been set by the fixtures
    user = user if user else USER_NO_ADMIN
    token = token if token else TOKEN_NO_ADMIN
    _set_up_workspace_objects(ws_controller, token)
    # need to get the mock objects first so spec_set can do its magic before we mock out
    # the classes in the context manager
    sub, schedd, txn = _get_htc_mocks()
    # seriously black you're killing me here. This is readable?
    with patch("htcondor.Submit", spec_set=True, autospec=True) as sub_init, patch(
        "htcondor.Schedd", spec_set=True, autospec=True
    ) as schedd_init, patch(
        CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True
    ) as list_cgroups, patch(
        CAT_GET_MODULE_VERSION, spec_set=True, autospec=True
    ) as get_mod_ver:
        # set up the rest of the mocks
        _finish_htc_mocks(sub_init, schedd_init, sub, schedd, txn)
        sub.queue.return_value = 123
        list_cgroups.return_value = catalog_return or []
        get_mod_ver.return_value = {"git_commit_hash": "somehash"}

        # run the method
        ee2 = ee2client(f"http://localhost:{ee2_port}", token=token)
        params = _get_run_job_param_set(app_id, job_reqs, as_admin)
        job_id = ee2.run_job(params)

        # check that mocks were called correctly
        # Since these are class methods, the first argument is self, which we ignore
        get_mod_ver.assert_called_once_with(
            ANY, {"module_name": "mod", "version": "beta"}
        )
        list_cgroups.assert_called_once_with(
            ANY, {"module_name": "mod", "function_name": "meth"}
        )

        expected_sub = _get_condor_sub_for_rj_param_set(
            job_id,
            user,
            token,
            clientgroup=clientgroup,
            cpu=cpu,
            mem=mem,
            disk=disk,
            app_id=app_id,
            app_module=app_mod,
        )
        modify_sub(expected_sub)
        _check_htc_calls(sub_init, sub, schedd_init, schedd, txn, expected_sub)

        _check_mongo_job(
            mongo_client,
            job_id,
            user,
            app_id,
            clientgroup=clientgroup,
            cpu=cpu,
            mem=mem,
            disk=disk,
            githash="somehash",
        )


def test_run_job_fail_not_admin(ee2_port):
    params = {"method": _MOD, "as_admin": 1}
    err = "Access Denied: You are not an administrator. AdminPermissions.NONE"
    _run_job_fail(ee2_port, TOKEN_NO_ADMIN, params, err)


def test_run_job_fail_no_workspace_access(ee2_port):
    params = {"method": _MOD, "wsid": 1}
    # this error could probably use some cleanup
    err = (
        "('An error occurred while fetching user permissions from the Workspace', "
        + "ServerError('No workspace with id 1 exists'))"
    )
    _run_job_fail(ee2_port, TOKEN_NO_ADMIN, params, err)


def test_run_job_fail_bad_cpu(ee2_port):
    params = {"method": _MOD, "job_requirements": {"request_cpus": -10}}
    err = "CPU count must be at least 1"
    _run_job_fail(ee2_port, TOKEN_WRITE_ADMIN, params, err)


def test_run_job_fail_bad_scheduler_requirements(ee2_port):
    params = {
        "method": _MOD,
        "job_requirements": {"scheduler_requirements": {"foo": ""}},
    }
    # TODO non-string keys/values in schd_reqs causes a not-very-useful error
    # Since it's admin only don't worry about it for now
    err = "Missing input parameter: value for key 'foo' in scheduler requirements structure"
    _run_job_fail(ee2_port, TOKEN_WRITE_ADMIN, params, err)


def test_run_job_fail_job_reqs_but_no_as_admin(ee2_port):
    params = {"method": _MOD, "job_requirements": {"request_cpus": 10}}
    err = "In order to specify job requirements you must be a full admin"
    _run_job_fail(ee2_port, TOKEN_NO_ADMIN, params, err)


def test_run_job_fail_bad_method(ee2_port):
    params = {"method": "mod.meth.moke"}
    err = "Unrecognized method: 'mod.meth.moke'. Please input module_name.function_name"
    _run_job_fail(ee2_port, TOKEN_NO_ADMIN, params, err)


def test_run_job_fail_bad_app(ee2_port):
    params = {"method": _MOD, "app_id": "mod.ap\bp"}
    err = "application ID contains control characters"
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_fail(ee2_port, TOKEN_NO_ADMIN, params, err)


def test_run_job_fail_bad_upa(ee2_port):
    params = {
        "method": _MOD,
        "source_ws_objects": ["ws/obj/1"],
    }
    err = (
        "source_ws_objects index 0, 'ws/obj/1', is not a valid Unique Permanent Address"
    )
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_fail(ee2_port, TOKEN_NO_ADMIN, params, err)


def test_run_job_fail_no_such_object(ee2_port, ws_controller):
    # Set up workspace and objects
    wsc = Workspace(ws_controller.get_url(), token=TOKEN_NO_ADMIN)
    wsc.create_workspace({"workspace": "foo"})
    wsc.save_objects(
        {
            "id": 1,
            "objects": [
                {"name": "one", "type": "Trivial.Object-1.0", "data": {}},
            ],
        }
    )
    params = {"method": _MOD, "source_ws_objects": ["1/2/1"]}
    err = "Some workspace object is inaccessible"
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_fail(ee2_port, TOKEN_NO_ADMIN, params, err)


def _run_job_fail(ee2_port, token, params, expected, throw_exception=False):
    client = ee2client(f"http://localhost:{ee2_port}", token=token)
    if throw_exception:
        client.run_job(params)
    else:
        with raises(ServerError) as got:
            client.run_job(params)
        assert_exception_correct(got.value, ServerError("name", 1, expected))


######## run_job_concierge tests ########


def test_run_job_concierge_minimal(ee2_port, ws_controller, mongo_client):
    def modify_sub(sub):
        del sub["Concurrency_Limits"]

    _run_job_concierge(
        ee2_port=ee2_port,
        ws_controller=ws_controller,
        mongo_client=mongo_client,
        # if the concierge dict is empty, regular old run_job gets run
        conc_params={"trigger": "concierge"},  # contents are ignored
        modify_sub=modify_sub,
        clientgroup="concierge",
        cpu=4,
        mem=23000,
        disk=100,
    )


def test_run_job_concierge_mixed(ee2_port, ws_controller, mongo_client):
    """
    Gets cpu from the input, memory from deploy.cfg, and disk from the catalog.
    """

    def modify_sub(sub):
        del sub["Concurrency_Limits"]

    _run_job_concierge(
        ee2_port=ee2_port,
        ws_controller=ws_controller,
        mongo_client=mongo_client,
        conc_params={"client_group": "extreme", "request_cpus": 76},
        modify_sub=modify_sub,
        clientgroup="extreme",
        cpu=76,
        mem=250000,
        disk=7,
        catalog_return=[{"client_groups": ['{"request_cpus":8,"request_disk":7}']}],
    )


def test_run_job_concierge_maximal(ee2_port, ws_controller, mongo_client):
    def modify_sub(sub):
        sub[
            "requirements"
        ] = '(CLIENTGROUP == "bigmem") && (baz == "bat") && (foo == "bar")'
        sub["Concurrency_Limits"] = "some_sucker"
        sub["+AccountingGroup"] = '"some_sucker"'
        sub["environment"] = sub["environment"].replace(
            "DEBUG_MODE=False", "DEBUG_MODE=True"
        )

    _run_job_concierge(
        ee2_port=ee2_port,
        ws_controller=ws_controller,
        mongo_client=mongo_client,
        conc_params={
            "client_group": "bigmem",
            "request_cpus": 42,
            "request_memory": 56,
            "request_disk": 89,
            "client_group_regex": False,
            "account_group": "some_sucker",
            "ignore_concurrency_limits": False,
            "requirements_list": ["foo=bar", "baz=bat"],
            "debug_mode": "true",
        },
        modify_sub=modify_sub,
        clientgroup="bigmem",
        cpu=42,
        mem=56,
        disk=89,
    )


def _run_job_concierge(
    ee2_port,
    ws_controller,
    mongo_client,
    conc_params,
    modify_sub,
    clientgroup,
    cpu,
    mem,
    disk,
    catalog_return=None,
):
    _set_up_workspace_objects(ws_controller, TOKEN_KBASE_CONCIERGE)
    # need to get the mock objects first so spec_set can do its magic before we mock out
    # the classes in the context manager
    sub, schedd, txn = _get_htc_mocks()
    # seriously black you're killing me here. This is readable?
    with patch("htcondor.Submit", spec_set=True, autospec=True) as sub_init, patch(
        "htcondor.Schedd", spec_set=True, autospec=True
    ) as schedd_init, patch(
        CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True
    ) as list_cgroups, patch(
        CAT_GET_MODULE_VERSION, spec_set=True, autospec=True
    ) as get_mod_ver:
        # set up the rest of the mocks
        _finish_htc_mocks(sub_init, schedd_init, sub, schedd, txn)
        sub.queue.return_value = 123
        list_cgroups.return_value = catalog_return or []
        get_mod_ver.return_value = {"git_commit_hash": "somehash"}

        # run the method
        ee2 = ee2client(f"http://localhost:{ee2_port}", token=TOKEN_KBASE_CONCIERGE)
        # if the concierge dict is empty, regular old run_job gets run
        job_id = ee2.run_job_concierge(_get_run_job_param_set(), conc_params)

        # check that mocks were called correctly
        # Since these are class methods, the first argument is self, which we ignore
        get_mod_ver.assert_called_once_with(
            ANY, {"module_name": "mod", "version": "beta"}
        )
        list_cgroups.assert_called_once_with(
            ANY, {"module_name": "mod", "function_name": "meth"}
        )

        expected_sub = _get_condor_sub_for_rj_param_set(
            job_id,
            USER_KBASE_CONCIERGE,
            TOKEN_KBASE_CONCIERGE,
            clientgroup,
            cpu,
            mem,
            disk,
        )
        modify_sub(expected_sub)

        _check_htc_calls(sub_init, sub, schedd_init, schedd, txn, expected_sub)

        _check_mongo_job(
            mongo_client,
            job_id,
            USER_KBASE_CONCIERGE,
            app_id="mod/app",
            clientgroup=clientgroup,
            cpu=cpu,
            mem=mem,
            disk=disk,
            githash="somehash",
        )


def test_run_job_concierge_fail_no_workspace_access(ee2_port):
    params = {"method": _MOD, "wsid": 1}
    # this error could probably use some cleanup
    err = (
        "('An error occurred while fetching user permissions from the Workspace', "
        + "ServerError('No workspace with id 1 exists'))"
    )
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, {"a": "b"}, err)


def test_run_job_concierge_fail_not_concierge(ee2_port):
    params = {"method": _MOD}
    err = "You are not the concierge user. This method is not for you"
    _run_job_concierge_fail(ee2_port, TOKEN_NO_ADMIN, params, {"a": "b"}, err)


def test_run_job_concierge_fail_bad_method(ee2_port):
    params = {"method": "mod.meth.moke"}
    err = "Unrecognized method: 'mod.meth.moke'. Please input module_name.function_name"
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, {"a": "b"}, err)


def test_run_job_concierge_fail_reqs_list_not_list(ee2_port):
    params = {"method": _MOD}
    conc_params = {"requirements_list": {"a": "b"}}
    err = "requirements_list must be a list"
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err)


def test_run_job_concierge_fail_reqs_list_bad_req(ee2_port):
    params = {"method": _MOD}
    conc_params = {"requirements_list": ["a=b", "touchmymonkey"]}
    err = "Found illegal requirement in requirements_list: touchmymonkey"
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err)


def test_run_job_concierge_fail_bad_cpu(ee2_port):
    params = {"method": _MOD}
    conc_params = {"request_cpus": [2]}
    err = (
        "Found illegal cpu request '[2]' in job requirements from concierge parameters"
    )
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err)


def test_run_job_concierge_fail_bad_mem(ee2_port):
    params = {"method": _MOD}
    conc_params = {"request_memory": "-3"}
    err = "memory in MB must be at least 1"
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err)


def test_run_job_concierge_fail_bad_disk(ee2_port):
    params = {"method": _MOD}
    conc_params = {"request_disk": 4.5}
    err = (
        "Found illegal disk request '4.5' in job requirements from concierge parameters"
    )
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err)


def test_run_job_concierge_fail_bad_clientgroup(ee2_port):
    params = {"method": _MOD}
    conc_params = {"client_group": "fakefakefake"}
    err = "No such clientgroup: fakefakefake"
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_concierge_fail(
            ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err
        )


def test_run_job_concierge_fail_bad_clientgroup_regex(ee2_port):
    params = {"method": _MOD}
    conc_params = {"client_group_regex": "now I have 2 problems"}
    err = (
        "Found illegal client group regex 'now I have 2 problems' in job requirements "
        + "from concierge parameters"
    )
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err)


def test_run_job_concierge_fail_bad_catalog_data(ee2_port):
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = [{"client_groups": ['{"request_cpus":-8}']}]

        params = {"method": _MOD}
        conc_params = {"request_memory": 9}
        # TODO this is not a useful error for the user. Need to change the job reqs resolver
        # However, getting this wrong in the catalog is not super likely so not urgent
        err = "CPU count must be at least 1"
        _run_job_concierge_fail(
            ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err
        )


def test_run_job_concierge_fail_bad_reqs_item(ee2_port):
    params = {"method": _MOD}
    conc_params = {"requirements_list": ["a=b", "=c"]}
    # this error isn't the greatest but as I understand it the concierge endpoint is going
    # to become redundant so don't worry about it for now
    err = "Missing input parameter: key in scheduler requirements structure"
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err)


def test_run_job_concierge_fail_bad_debug_mode(ee2_port):
    params = {"method": _MOD}
    conc_params = {"debug_mode": "debug debug debug"}
    err = (
        "Found illegal debug mode 'debug debug debug' in job requirements from "
        + "concierge parameters"
    )
    _run_job_concierge_fail(ee2_port, TOKEN_KBASE_CONCIERGE, params, conc_params, err)


def test_run_job_concierge_fail_bad_app(ee2_port):
    params = {"method": _MOD, "app_id": "mo\bd.app"}
    err = "application ID contains control characters"
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_concierge_fail(
            ee2_port, TOKEN_KBASE_CONCIERGE, params, {"a": "b"}, err
        )


def test_run_job_concierge_fail_bad_upa(ee2_port):
    params = {
        "method": _MOD,
        "source_ws_objects": ["ws/obj/1"],
    }
    err = (
        "source_ws_objects index 0, 'ws/obj/1', is not a valid Unique Permanent Address"
    )
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_concierge_fail(
            ee2_port, TOKEN_KBASE_CONCIERGE, params, {"a": "b"}, err
        )


def test_run_job_concierge_fail_no_such_object(ee2_port, ws_controller):
    # Set up workspace and objects
    wsc = Workspace(ws_controller.get_url(), token=TOKEN_NO_ADMIN)
    wsc.create_workspace({"workspace": "foo"})
    wsc.save_objects(
        {
            "id": 1,
            "objects": [
                {"name": "one", "type": "Trivial.Object-1.0", "data": {}},
            ],
        }
    )
    params = {"method": _MOD, "source_ws_objects": ["1/2/1"]}
    err = "Some workspace object is inaccessible"
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_concierge_fail(
            ee2_port, TOKEN_KBASE_CONCIERGE, params, {"a": "b"}, err
        )


def _run_job_concierge_fail(
    ee2_port, token, params, conc_params, expected, throw_exception=False
):
    client = ee2client(f"http://localhost:{ee2_port}", token=token)
    if throw_exception:
        client.run_job_concierge(params, conc_params)
    else:
        with raises(ServerError) as got:
            client.run_job_concierge(params, conc_params)
        assert_exception_correct(got.value, ServerError("name", 1, expected))


######## run_job_batch tests ########


def test_run_job_batch(ee2_port, ws_controller, mongo_client):
    """
    A test of the run_job method.
    """
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN, "foo")  # ws 1
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN, "bar")  # ws 2
    # need to get the mock objects first so spec_set can do its magic before we mock out
    # the classes in the context manager
    sub, schedd, txn = _get_htc_mocks()
    # seriously black you're killing me here. This is readable?
    with patch("htcondor.Submit", spec_set=True, autospec=True) as sub_init, patch(
        "htcondor.Schedd", spec_set=True, autospec=True
    ) as schedd_init, patch(
        CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True
    ) as list_cgroups, patch(
        CAT_GET_MODULE_VERSION, spec_set=True, autospec=True
    ) as get_mod_ver:
        # set up the rest of the mocks
        _finish_htc_mocks(sub_init, schedd_init, sub, schedd, txn)
        sub.queue.side_effect = [123, 456]
        list_cgroups.side_effect = [
            [{"client_groups": ['{"request_cpus":8,"request_memory":5}']}],
            [{"client_groups": ['{"client_group":"bigmem"}']}],
        ]
        get_mod_ver.side_effect = [
            {"git_commit_hash": "somehash"},
            {"git_commit_hash": "somehash2"},
        ]

        # run the method
        job1_params = {
            "method": _MOD,
            "source_ws_objects": ["1/1/1", "1/2/1"],
            "params": [{"foo": "bar"}, 42],
            "service_ver": "beta",
            "meta": {
                "run_id": "rid",
                "token_id": "tid",
                "tag": "yourit",
                "cell_id": "cid",
                "thiskey": "getssilentlydropped",
            },
        }
        job2_params = {
            "method": "mod2.meth2",
            "app_id": "mod2/app2",
            "wsid": 1,
            "params": [{"baz": "bat"}, 3.14],
        }
        job_batch_params = {
            "wsid": 2,
            "meta": {
                "run_id": "rid2",
                "token_id": "tid2",
                "tag": "yourit2",
                "cell_id": "cid2",
                "thiskey": "getssilentlydropped2",
            },
        }
        ee2 = ee2client(f"http://localhost:{ee2_port}", token=TOKEN_NO_ADMIN)
        ret = ee2.run_job_batch([job1_params, job2_params], job_batch_params)
        parent_job_id = ret["parent_job_id"]
        job_id_1, job_id_2 = ret["child_job_ids"]

        # check that mocks were called correctly
        # Since these are class methods, the first argument is self, which we ignore
        get_mod_ver.assert_has_calls(
            [
                call(ANY, {"module_name": "mod", "version": "beta"}),
                call(ANY, {"module_name": "mod2", "version": "release"}),
            ]
        )
        list_cgroups.assert_has_calls(
            [
                call(ANY, {"module_name": "mod", "function_name": "meth"}),
                call(ANY, {"module_name": "mod2", "function_name": "meth2"}),
            ]
        )

        job1 = _get_mongo_job(mongo_client, job_id_1)
        job2 = _get_mongo_job(mongo_client, job_id_2)

        expected_job1 = {
            "_id": ObjectId(job_id_1),
            "user": USER_NO_ADMIN,
            "authstrat": "kbaseworkspace",
            "status": "queued",
            "job_input": {
                "method": _MOD,
                "params": [{"foo": "bar"}, 42],
                "service_ver": "somehash",
                "source_ws_objects": ["1/1/1", "1/2/1"],
                "parent_job_id": parent_job_id,
                "requirements": {
                    "clientgroup": "njs",
                    "cpu": 8,
                    "memory": 5,
                    "disk": 30,
                },
                "narrative_cell_info": {
                    "run_id": "rid",
                    "token_id": "tid",
                    "tag": "yourit",
                    "cell_id": "cid",
                },
            },
            "child_jobs": [],
            "batch_job": False,
            "scheduler_id": "123",
            "scheduler_type": "condor",
        }
        assert job1 == expected_job1

        expected_job2 = {
            "_id": ObjectId(job_id_2),
            "user": USER_NO_ADMIN,
            "authstrat": "kbaseworkspace",
            "wsid": 1,
            "status": "queued",
            "job_input": {
                "wsid": 1,
                "method": "mod2.meth2",
                "params": [{"baz": "bat"}, 3.14],
                "service_ver": "somehash2",
                "app_id": "mod2/app2",
                "source_ws_objects": [],
                "parent_job_id": parent_job_id,
                "requirements": {
                    "clientgroup": "bigmem",
                    "cpu": 4,
                    "memory": 2000,
                    "disk": 100,
                },
                "narrative_cell_info": {},
            },
            "child_jobs": [],
            "batch_job": False,
            "scheduler_id": "456",
            "scheduler_type": "condor",
        }
        assert job2 == expected_job2

        parent_job = _get_mongo_job(mongo_client, parent_job_id, has_queued=False)
        expected_parent_job = {
            "_id": ObjectId(parent_job_id),
            "user": USER_NO_ADMIN,
            "authstrat": "kbaseworkspace",
            "wsid": 2,
            "status": "created",
            "job_input": {
                "method": "batch",
                "service_ver": "batch",
                "app_id": "batch",
                "source_ws_objects": [],
                "narrative_cell_info": {
                    "run_id": "rid2",
                    "token_id": "tid2",
                    "tag": "yourit2",
                    "cell_id": "cid2",
                },
            },
            "child_jobs": [job_id_1, job_id_2],
            "batch_job": True,
        }
        assert parent_job == expected_parent_job

        expected_sub_1 = _get_condor_sub_for_rj_param_set(
            job_id_1,
            USER_NO_ADMIN,
            TOKEN_NO_ADMIN,
            clientgroup="njs",
            cpu=8,
            mem=5,
            disk=30,
            parent_job_id=parent_job_id,
            app_id=None,
            app_module=None,
        )
        expected_sub_1["+KB_WSID"] = ""
        expected_sub_2 = _get_condor_sub_for_rj_param_set(
            job_id_2,
            USER_NO_ADMIN,
            TOKEN_NO_ADMIN,
            clientgroup="bigmem",
            cpu=4,
            mem=2000,
            disk=100,
            parent_job_id=parent_job_id,
        )
        expected_sub_2.update(
            {
                "+KB_MODULE_NAME": '"mod2"',
                "+KB_FUNCTION_NAME": '"meth2"',
                "+KB_APP_ID": '"mod2/app2"',
                "+KB_APP_MODULE_NAME": '"mod2"',
                "+KB_SOURCE_WS_OBJECTS": "",
            }
        )
        _check_batch_htc_calls(
            sub_init, schedd_init, sub, schedd, txn, expected_sub_1, expected_sub_2
        )


def test_run_job_batch_as_admin_with_job_reqs(ee2_port, ws_controller, mongo_client):
    """
    A test of the run_job method focusing on job requirements and minimizing all other inputs.
    Since the batch endpoint uses the same code path as the single job endpoint for processing
    job requirements, we only have a single test that mixes job requirements from the input,
    catalog, and deploy configuration, as opposed to the multiple tests for single jobs.
    """
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN, "foo")  # ws 1
    # need to get the mock objects first so spec_set can do its magic before we mock out
    # the classes in the context manager
    sub, schedd, txn = _get_htc_mocks()
    # seriously black you're killing me here. This is readable?
    with patch("htcondor.Submit", spec_set=True, autospec=True) as sub_init, patch(
        "htcondor.Schedd", spec_set=True, autospec=True
    ) as schedd_init, patch(
        CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True
    ) as list_cgroups, patch(
        CAT_GET_MODULE_VERSION, spec_set=True, autospec=True
    ) as get_mod_ver:
        # set up the rest of the mocks
        _finish_htc_mocks(sub_init, schedd_init, sub, schedd, txn)
        sub.queue.side_effect = [123, 456]
        list_cgroups.side_effect = [
            [{"client_groups": ['{"client_group":"bigmem"}']}],
            [{"client_groups": ['{"request_disk":8,"request_memory":5}']}],
        ]
        get_mod_ver.side_effect = [
            {"git_commit_hash": "somehash"},
            {"git_commit_hash": "somehash2"},
        ]

        # run the method
        job1_params = {"method": _MOD}
        job2_params = {
            "method": "mod2.meth2",
            "job_requirements": {
                "request_memory": 42,
                "client_group": "extreme",
                "client_group_regex": 0,
                "bill_to_user": "forrest_gump",
                "ignore_concurrency_limits": "true",
                "scheduler_requirements": {"foo": "bar", "baz": "bat"},
                "debug_mode": True,
            },
        }
        job_batch_params = {"wsid": 1, "as_admin": "foo"}
        ee2 = ee2client(f"http://localhost:{ee2_port}", token=TOKEN_WRITE_ADMIN)
        ret = ee2.run_job_batch([job1_params, job2_params], job_batch_params)
        parent_job_id = ret["parent_job_id"]
        job_id_1, job_id_2 = ret["child_job_ids"]

        # check that mocks were called correctly
        # Since these are class methods, the first argument is self, which we ignore
        get_mod_ver.assert_has_calls(
            [
                call(ANY, {"module_name": "mod", "version": "release"}),
                call(ANY, {"module_name": "mod2", "version": "release"}),
            ]
        )
        list_cgroups.assert_has_calls(
            [
                call(ANY, {"module_name": "mod", "function_name": "meth"}),
                call(ANY, {"module_name": "mod2", "function_name": "meth2"}),
            ]
        )

        job1 = _get_mongo_job(mongo_client, job_id_1)
        job2 = _get_mongo_job(mongo_client, job_id_2)

        expected_job1 = {
            "_id": ObjectId(job_id_1),
            "user": USER_WRITE_ADMIN,
            "authstrat": "kbaseworkspace",
            "status": "queued",
            "job_input": {
                "method": _MOD,
                "service_ver": "somehash",
                "source_ws_objects": [],
                "parent_job_id": parent_job_id,
                "requirements": {
                    "clientgroup": "bigmem",
                    "cpu": 4,
                    "memory": 2000,
                    "disk": 100,
                },
                "narrative_cell_info": {},
            },
            "child_jobs": [],
            "batch_job": False,
            "scheduler_id": "123",
            "scheduler_type": "condor",
        }
        assert job1 == expected_job1

        expected_job2 = {
            "_id": ObjectId(job_id_2),
            "user": USER_WRITE_ADMIN,
            "authstrat": "kbaseworkspace",
            "status": "queued",
            "job_input": {
                "method": "mod2.meth2",
                "service_ver": "somehash2",
                "source_ws_objects": [],
                "parent_job_id": parent_job_id,
                "requirements": {
                    "clientgroup": "extreme",
                    "cpu": 32,
                    "memory": 42,
                    "disk": 8,
                },
                "narrative_cell_info": {},
            },
            "child_jobs": [],
            "batch_job": False,
            "scheduler_id": "456",
            "scheduler_type": "condor",
        }
        assert job2 == expected_job2

        parent_job = _get_mongo_job(mongo_client, parent_job_id, has_queued=False)
        expected_parent_job = {
            "_id": ObjectId(parent_job_id),
            "user": USER_WRITE_ADMIN,
            "authstrat": "kbaseworkspace",
            "wsid": 1,
            "status": "created",
            "job_input": {
                "method": "batch",
                "service_ver": "batch",
                "app_id": "batch",
                "source_ws_objects": [],
                "narrative_cell_info": {},
            },
            "child_jobs": [job_id_1, job_id_2],
            "batch_job": True,
        }
        assert parent_job == expected_parent_job

        expected_sub_1 = _get_condor_sub_for_rj_param_set(
            job_id_1,
            USER_WRITE_ADMIN,
            TOKEN_WRITE_ADMIN,
            clientgroup="bigmem",
            cpu=4,
            mem=2000,
            disk=100,
            parent_job_id=parent_job_id,
            app_id=None,
            app_module=None,
        )
        expected_sub_1.update({"+KB_SOURCE_WS_OBJECTS": "", "+KB_WSID": ""})
        expected_sub_2 = _get_condor_sub_for_rj_param_set(
            job_id_2,
            USER_WRITE_ADMIN,
            TOKEN_WRITE_ADMIN,
            clientgroup="extreme",
            cpu=32,
            mem=42,
            disk=8,
            parent_job_id=parent_job_id,
            app_id=None,
            app_module=None,
        )
        expected_sub_2.update(
            {
                "+KB_SOURCE_WS_OBJECTS": "",
                "+KB_WSID": "",
                "+AccountingGroup": '"forrest_gump"',
                "+KB_MODULE_NAME": '"mod2"',
                "+KB_FUNCTION_NAME": '"meth2"',
                "requirements": '(CLIENTGROUP == "extreme") && (baz == "bat") && (foo == "bar")',
                "environment": expected_sub_2["environment"].replace(
                    "DEBUG_MODE=False", "DEBUG_MODE=True"
                ),
            }
        )
        del expected_sub_2["Concurrency_Limits"]
        _check_batch_htc_calls(
            sub_init, schedd_init, sub, schedd, txn, expected_sub_1, expected_sub_2
        )


def _check_batch_htc_calls(
    sub_init, schedd_init, sub, schedd, txn, expected_sub_1, expected_sub_2
):
    assert sub_init.call_args_list == [call(expected_sub_1), call(expected_sub_2)]
    # The line above and the line below should be completely equivalent IIUC, but the line
    # below fails for reasons I don't understand. The error output shows the actual calls
    # for the line below having 2 extra calls that appear to be the sub.queue calls
    # below. Stumped, so going with what works and moving on.
    # sub_init.assert_has_calls([call(expected_sub_1), call(expected_sub_2)])
    schedd_init.call_args_list = [call(), call()]
    # same deal here. Output includes stuff like `call().transaction()` so
    # it appears the sub calls are being picked up, which is weird.
    # schedd_init.assert_has_calls([call(), call()])
    schedd.transaction.call_args_list = [call(), call()]
    # and again
    # schedd.transaction.assert_has_calls([call(), call()])
    sub.queue.assert_has_calls([call(txn, 1), call(txn, 1)])


def test_run_job_batch_fail_not_admin(ee2_port, ws_controller):
    err = "Access Denied: You are not an administrator. AdminPermissions.NONE"
    _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, [], {"as_admin": True}, err)


def test_run_job_batch_fail_no_workspace_access_for_batch(ee2_port, ws_controller):
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN)
    params = [{"method": _MOD}]
    # this error could probably use some cleanup
    err = (
        "('An error occurred while fetching user permissions from the Workspace', "
        + "ServerError('No workspace with id 2 exists'))"
    )
    _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 2}, err)


def test_run_job_batch_fail_no_workspace_access_for_job(ee2_port):
    params = [
        {"method": _MOD},
        {"method": _MOD, "wsid": 1},
    ]
    # this error could probably use some cleanup
    err = (
        "('An error occurred while fetching user permissions from the Workspace', "
        + "ServerError('No workspace with id 1 exists'))"
    )
    _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def test_run_job_batch_fail_bad_memory(ee2_port, ws_controller):
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN)
    params = [
        {"method": _MOD},
        {"method": _MOD},
        {"method": _MOD, "job_requirements": {"request_memory": [1000]}},
    ]
    err = "Job #3: Found illegal memory request '[1000]' in job requirements from input job"
    _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def test_run_job_batch_fail_bad_scheduler_requirements(ee2_port, ws_controller):
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN)
    params = [
        {"method": _MOD, "job_requirements": {"scheduler_requirements": {"": "foo"}}},
        {"method": _MOD},
    ]
    err = "Job #1: Missing input parameter: key in scheduler requirements structure"
    _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def test_run_job_batch_fail_job_reqs_but_no_as_admin(ee2_port, ws_controller):
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN)
    params = [
        {"method": _MOD},
        {
            "method": _MOD,
            "job_requirements": {"request_memory": 1000},
            # as_admin is only considered in the batch params for run_job_batch
            "as_admin": True,
        },
    ]
    err = "Job #2: In order to specify job requirements you must be a full admin"
    _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def test_run_job_batch_fail_bad_catalog_data(ee2_port, ws_controller):
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN)
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = [{"client_groups": ['{"request_cpus":-8}']}]

        params = [{"method": _MOD}]
        # TODO this is not a useful error for the user. Need to change the job reqs resolver
        # However, getting this wrong in the catalog is not super likely so not urgent
        err = "CPU count must be at least 1"
        _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def test_run_job_batch_fail_bad_method(ee2_port, ws_controller):
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN)
    params = [
        {"method": _MOD},
        {"method": "mod.meth.moke"},
    ]
    err = "Job #2: Unrecognized method: 'mod.meth.moke'. Please input module_name.function_name"
    # TODO this test surfaced a bug - if a batch wsid is not supplied and any job does not have
    # a wsid an error occurs
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def test_run_job_batch_fail_bad_app(ee2_port, ws_controller):
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN)
    params = [{"method": _MOD, "app_id": "mod.\bapp"}]
    err = "application ID contains control characters"
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def test_run_job_batch_fail_bad_upa(ee2_port, ws_controller):
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN)
    params = [
        {
            "method": _MOD,
            "source_ws_objects": ["ws/obj/1"],
        }
    ]
    err = (
        "source_ws_objects index 0, 'ws/obj/1', is not a valid Unique Permanent Address"
    )
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def test_run_job_batch_fail_parent_id(ee2_port, ws_controller):
    _set_up_workspace_objects(ws_controller, TOKEN_NO_ADMIN)

    params = [{"method": _MOD, "parent_job_id": "ae"}]
    err = "batch jobs may not specify a parent job ID"
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)

    params = [
        {"method": _MOD},
        {"method": _MOD, "parent_job_id": "ae"},
    ]
    err = "Job #2: batch jobs may not specify a parent job ID"
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def test_run_job_batch_fail_no_such_object(ee2_port, ws_controller):
    # Set up workspace and objects
    wsc = Workspace(ws_controller.get_url(), token=TOKEN_NO_ADMIN)
    wsc.create_workspace({"workspace": "foo"})
    wsc.save_objects(
        {
            "id": 1,
            "objects": [
                {"name": "one", "type": "Trivial.Object-1.0", "data": {}},
            ],
        }
    )
    params = [{"method": _MOD, "source_ws_objects": ["1/2/1"]}]
    err = "Some workspace object is inaccessible"
    with patch(CAT_LIST_CLIENT_GROUPS, spec_set=True, autospec=True) as list_cgroups:
        list_cgroups.return_value = []
        _run_job_batch_fail(ee2_port, TOKEN_NO_ADMIN, params, {"wsid": 1}, err)


def _run_job_batch_fail(
    ee2_port, token, params, batch_params, expected, throw_exception=False
):
    client = ee2client(f"http://localhost:{ee2_port}", token=token)
    if throw_exception:
        client.run_job_batch(params, batch_params)
    else:
        with raises(ServerError) as got:
            client.run_job_batch(params, batch_params)
        assert_exception_correct(got.value, ServerError("name", 1, expected))
