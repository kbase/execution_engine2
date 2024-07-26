# -*- coding: utf-8 -*-
import os
import unittest
from configparser import ConfigParser
from unittest.mock import create_autospec

import bson
from mock import MagicMock
from mock import patch

from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.db.models.models import Status
from execution_engine2.sdk.EE2Constants import ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
from execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.CondorTuples import SubmissionInfo
from execution_engine2.utils.clients import (
    UserClientSet,
    ClientSet,
    get_client_set,
    get_user_client_set,
)
from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace
from test.utils_shared.mock_utils import get_client_mocks as _get_client_mocks
from test.utils_shared.test_utils import (
    get_sample_job_params,
    get_sample_condor_info,
)

# Cause any tests that contact external services (e.g. KBASE CI auth) as part of the test to
# pass automatically.
SKIP_TESTS_WITH_EXTERNALITIES = False


class EE2TestAdminMode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
        config_parser = ConfigParser()
        config_parser.read(cls.config_file)

        cls.cfg = {}

        for nameval in config_parser.items("execution_engine2"):
            cls.cfg[nameval[0]] = nameval[1]

        mongo_in_docker = cls.cfg.get("mongo-in-docker-compose", None)
        if mongo_in_docker is not None:
            cls.cfg["mongo-host"] = cls.cfg["mongo-in-docker-compose"]

        cls.user_id = "wsadmin"
        cls.ws_id = 9999
        cls.token = "token"

    def setUp(self) -> None:
        """
        Patch out Condor
        :return:
        """
        si = SubmissionInfo(clusterid="123", submit={}, error=None)
        self.condor_patch = patch.object(
            target=Condor, attribute="run_job", return_value=si
        )
        self.condor_patch2 = patch.object(
            target=Condor,
            attribute="_get_job_info",
            return_value=get_sample_condor_info(),
        )

        self.mock_condor = self.condor_patch.start()
        self.mock_condor2 = self.condor_patch2.start()

        self.setup_runner = self.getRunner()
        self.method_1 = "module_name.function_name"
        self.job_params_1 = get_sample_job_params(method=self.method_1, wsid=self.ws_id)

        # TODO
        # PATCH OUT LOGIN/WORKSPACE HERE
        # self.good_job_id_user1 = setup_runner.run_job(params=job_params_1,as_admin=False)
        # self.good_job_id_user2 = setup_runner.run_job(params=job_params_1,as_admin=False)

    def tearDown(self) -> None:
        self.condor_patch.stop()
        self.condor_patch2.start()

    def getRunner(self, user_clients=None, clients=None) -> SDKMethodRunner:
        # Initialize these clients from None
        if not user_clients:
            user_clients = get_user_client_set(self.cfg, self.user_id, self.token)
        if not clients:
            with open(self.config_file) as cf:
                clients = get_client_set(self.cfg, cf)
        runner = SDKMethodRunner(user_clients, clients)  # type : SDKMethodRunner
        runner.get_jobs_status()
        runner.get_runjob()
        runner.get_job_logs()

        return runner

    def get_runner_with_condor(self) -> SDKMethodRunner:
        runner = self.getRunner()
        condor = MagicMock(return_value={})
        condor._get_job_info = MagicMock(return_value="")
        condor.get_job_resource_info = MagicMock(return_value="njs")
        runner.condor = condor

        return runner

    # TODO How do you test ADMIN_MODE without increasing too much coverage

    def get_user_mocks(
        self, user_id=None, token=None
    ) -> (UserClientSet, Workspace, WorkspaceAuth):
        user_id = user_id if user_id else self.user_id
        token = token if token else self.token
        ws = create_autospec(Workspace, instance=True, spec_set=True)
        wsa = create_autospec(WorkspaceAuth, instance=True, spec_set=True)
        ucs = UserClientSet(user_id, token, ws, wsa)
        return ucs, ws, wsa

    def get_client_mocks(self, *to_be_mocked):
        return _get_client_mocks(self.cfg, self.config_file, *to_be_mocked)

    def test_regular_user(self):
        # Regular User
        lowly_user = "Access Denied: You are not an administrator"
        user_client_set, _, ws_auth = self.get_user_mocks()
        clients_and_mocks = self.get_client_mocks(AdminAuthUtil, Catalog)
        aau = clients_and_mocks[AdminAuthUtil]
        catalog = clients_and_mocks[Catalog]
        # TODO check catalog called as expected
        catalog.get_module_version.return_value = {
            "git_commit_hash": "moduleversiongoeshere"
        }
        catalog.list_client_group_configs.return_value = []
        aau.get_admin_role.return_value = None
        ws_auth.can_write.return_value = True
        runner = self.getRunner(user_client_set, clients_and_mocks[ClientSet])
        method_1 = "module_name.function_name"
        job_params_1 = get_sample_job_params(
            method=method_1, wsid=self.ws_id, app_id="module_name/foo"
        )

        # Check Admin Status
        is_admin = runner.check_is_admin()
        self.assertFalse(is_admin)

        aau.get_admin_role.assert_called_once_with(
            self.token, ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
        )

        # Check Admin Status
        admin_type = runner.get_admin_permission()
        self.assertEqual(admin_type, {"permission": "n"})

        # RUNJOB

        job_id = runner.run_job(params=job_params_1, as_admin=False)
        self.assertTrue(bson.objectid.ObjectId.is_valid(job_id))
        ws_auth.can_write.assert_called_once_with(self.ws_id)

        # RUNJOB BUT ATTEMPT TO BE AN ADMIN
        with self.assertRaisesRegex(
            expected_exception=PermissionError, expected_regex=lowly_user
        ):
            runner.run_job(params=job_params_1, as_admin=True)

        # get_job_params
        params = runner.get_job_params(job_id=job_id)
        self.assertEqual(params["method"], job_params_1["method"])

        # get_job_params BUT ATTEMPT TO BE AN ADMIN
        with self.assertRaisesRegex(
            expected_exception=PermissionError, expected_regex=lowly_user
        ):
            runner.get_job_params(job_id=job_id, as_admin=True)

        # LOGS #
        # add_job_logs and view them
        lines = []
        for item in ["this", "is", "a", "test"]:
            line = {"error": False, "line": item}
            lines.append(line)

        runner.add_job_logs(job_id=job_id, log_lines=lines)
        runner.view_job_logs(job_id=job_id)

        # add_job_logs and view them, BUT ATTEMPT TO BE AN ADMIN
        with self.assertRaisesRegex(
            expected_exception=PermissionError, expected_regex=lowly_user
        ):
            runner.add_job_logs(job_id=job_id, log_lines=lines, as_admin=True)

        with self.assertRaisesRegex(
            expected_exception=PermissionError, expected_regex=lowly_user
        ):
            runner.view_job_logs(job_id=job_id, as_admin=True)

        # Start the job and get it's status
        runner.start_job(job_id=job_id)
        status_field = runner.get_job_status_field(job_id=job_id)
        self.assertEqual(status_field["status"], Status.running.value)
        runner.finish_job(job_id=job_id, error_message="Fail")
        check_job = runner.check_job(job_id=job_id)
        self.assertEqual(check_job["status"], Status.error.value)
        job_id2 = runner.run_job(params=job_params_1, as_admin=False)
        self.assertTrue(bson.objectid.ObjectId.is_valid(job_id2))
        runner.cancel_job(job_id=job_id2)
        check_job2 = runner.check_job(job_id=job_id2)
        self.assertEqual(check_job2["status"], Status.terminated.value)

        # TODO do the above with as_admin=True and assert failure each time

        # Start the job and get its status as an admin

    @patch.object(WorkspaceAuth, "can_write", return_value=True)
    def test_admin_writer(self, workspace):
        # Admin User with WRITE

        clients_and_mocks = self.get_client_mocks(AdminAuthUtil, Catalog)
        clients = clients_and_mocks[ClientSet]
        adminauth = clients_and_mocks[AdminAuthUtil]
        catalog = clients_and_mocks[Catalog]
        # TODO check catalog called as expected
        catalog.get_module_version.return_value = {
            "git_commit_hash": "moduleversiongoeshere"
        }
        catalog.list_client_group_configs.return_value = []

        runner = self.getRunner(None, clients)
        adminauth.get_admin_role.return_value = ADMIN_READ_ROLE
        method_1 = "module_name.function_name"
        job_params_1 = get_sample_job_params(method=method_1, wsid=self.ws_id)

        # Check Admin Status
        is_admin = runner.check_is_admin()
        self.assertTrue(is_admin)

        adminauth.get_admin_role.assert_called_once_with(
            self.token, ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
        )

        # Admin User with WRITE

        runner = self.getRunner(None, clients)
        # SET YOUR ADMIN STATUS HERE
        adminauth.get_admin_role.return_value = ADMIN_WRITE_ROLE

        method_1 = "module_name.function_name"
        job_params_1 = get_sample_job_params(
            method=method_1, wsid=self.ws_id, app_id="module_name/foo"
        )

        # Check Admin Status
        is_admin = runner.check_is_admin()
        self.assertTrue(is_admin)

        # Check Admin Status
        admin_type = runner.get_admin_permission()
        self.assertEqual(admin_type, {"permission": "w"})

        # RUNJOB
        job_id = runner.run_job(params=job_params_1, as_admin=True)
        self.assertTrue(bson.objectid.ObjectId.is_valid(job_id))

        # CHECKJOB
        check_job = runner.check_job(job_id=job_id, as_admin=True)
        self.assertEqual(check_job.get("status"), Status.queued.value)

        # get_job_params
        params = runner.get_job_params(job_id=job_id, as_admin=True)
        self.assertEqual(params["method"], job_params_1["method"])

        # runner.handle_held_job(cluster_id=check_job.get("scheduler_id"))

    # These tests should throw the most errors

    def test_no_user(self):
        if SKIP_TESTS_WITH_EXTERNALITIES:
            return
        # Passes a fake token to the auth server, guaranteed to fail.
        # Auth is *not mocked*, hits the real auth service. Will fail if CI is down.
        # Not sure of the value of this test - if a client actually passes a bad token to the
        # server it'll get caught in the Server.py file before the Impl file is reached.
        runner = self.getRunner()
        method_1 = "module_name.function_name"
        job_params_1 = get_sample_job_params(method=method_1, wsid=self.ws_id)
        error_regex = (
            r"An error occurred while fetching user permissions from the Workspace"
            r", ServerError\('Token validation failed: Auth service returned an error: 10020 Invalid token'\)"
        )
        with self.assertRaisesRegex(
            expected_exception=RuntimeError, expected_regex=error_regex
        ):
            runner.run_job(params=job_params_1, as_admin=False)

    def test_admin_reader(self):
        # Admin User with READ
        lowly_admin = r"Access Denied: You are a read-only admin. This function requires write access"
        clients_and_mocks = self.get_client_mocks(AdminAuthUtil)
        adminauth = clients_and_mocks[AdminAuthUtil]
        runner = self.getRunner(None, clients_and_mocks[ClientSet])
        adminauth.get_admin_role.return_value = ADMIN_READ_ROLE
        method_1 = "module_name.function_name"
        job_params_1 = get_sample_job_params(method=method_1, wsid=self.ws_id)

        # Check Admin Status
        is_admin = runner.check_is_admin()
        self.assertTrue(is_admin)

        adminauth.get_admin_role.assert_called_once_with(
            self.token, ADMIN_READ_ROLE, ADMIN_WRITE_ROLE
        )

        # Check Admin Status
        admin_type = runner.get_admin_permission()
        self.assertEqual(admin_type, {"permission": "r"})

        # RUNJOB
        with self.assertRaisesRegex(
            expected_exception=PermissionError, expected_regex=lowly_admin
        ):
            runner.run_job(params=job_params_1, as_admin=True)
        #
        # good_job_id = 1
        # check_job = runner.check_job(job_id=good_job_id)
        # self.assertEqual(check_job.get("status"),Status.queued.value)
