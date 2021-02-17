"""
Integration tests that cover the entire codebase from API to database.

NOTE 1: These tests are designed to only be runnable after running docker-compose up

NOTE 2: These tests were set up quickly in order to debug a problem with administration related
calls. As such, the auth server was set up to run in test mode locally. If more integrations
(e.g. the workspace) are needed, they will need to be added either locally or as docker containers.
If the latter, the test auth integration will likely need to be converted to a docker container or
exposed to other containers.
"""

from pathlib import Path
import pymongo
from pytest import fixture
from tests_for_integration.auth_controller import AuthController
from utils_shared.test_utils import get_test_config

KEEP_TEMP_FILES = True
AUTH_DB = "api_to_db_test"
AUTH_MONGO_USER = "auth"
TEMP_DIR = Path("test_temp_can_delete")

# may need to make this configurable
JARS_DIR = Path("/opt/jars/lib/jars")


@fixture(scope="module")
def config():
    yield get_test_config()


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

    yield url

    print(f"shutting down auth, KEEP_TEMP_FILES={KEEP_TEMP_FILES}")
    auth.destroy(not KEEP_TEMP_FILES)

    # Because the tests are run with mongo in a persistent docker container via docker-compose,
    # we need to clean up after ourselves.
    _clean_auth_db(mongo_client)


# TODO start the ee2 service
# TODO wipe the ee2 database between every test


def test_is_admin(auth_url):
    import requests

    print(requests.get(auth_url).text)
    # TODO add a test
