# # -*- coding: utf-8 -*-
# import unittest
# import requests_mock
# from lib.execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
# from test.utils_shared.test_utils import bootstrap
# from mock import MagicMock
# from lib.execution_engine2.utils.CondorTuples import CondorResources
# bootstrap()
# import os
# from configparser import ConfigParser
# import copy
# from mock import patch
# from lib.execution_engine2.utils.Condor import Condor
# from lib.execution_engine2.utils.CondorTuples import SubmissionInfo
# from installed_clients.CatalogClient import Catalog
# from lib.execution_engine2.authorization.roles import AdminAuthUtil
# from lib.execution_engine2.authorization.workspaceauth import WorkspaceAuth
# import bson
# from lib.execution_engine2.db.models.models import Status
# from test.utils_shared.test_utils import run_job_adapter, get_sample_job_params
#
# bootstrap()
#
# class EE2TestAdminMode(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls):
#         config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
#         config_parser = ConfigParser()
#         config_parser.read(config_file)
#
#         cls.cfg = {}
#
#         for nameval in config_parser.items("execution_engine2"):
#             cls.cfg[nameval[0]] = nameval[1]
#
#         mongo_in_docker = cls.cfg.get("mongo-in-docker-compose", None)
#         if mongo_in_docker is not None:
#             cls.cfg["mongo-host"] = cls.cfg["mongo-in-docker-compose"]
#
#         cls.user_id = "wsadmin"
#         cls.ws_id = 9999
#         cls.token = "token"
#
#         cls.method_runner = SDKMethodRunner(
#             cls.cfg, user_id=cls.user_id, token=cls.token
#         )
#
#         cls.cr = CondorResources(
#             request_cpus="1",
#             request_disk="1GB",
#             request_memory="100M",
#             client_group="njs",
#         )
#         cls.ADMIN_WRITE_ROLE = "EE2_ADMIN"
#
#     def setUp(self) -> None:
#         """
#         Patch out Catalog and Condor
#         :return:
#         """
#         self.catalog_patch = patch(
#             "lib.installed_clients.CatalogClient.Catalog.get_module_version"
#         )
#         self.catalog = self.catalog_patch.start()
#         self.catalog.return_value = {"git_commit_hash": "moduleversiongoeshere"}
#
#         si = SubmissionInfo(clusterid="123", submit={'scheduler_id' : 123}, error=None)
#         self.condor_patch = patch.object(Condor, "run_job", return_value=si)
#         self.condor = self.condor_patch.start()
#
#         self.setup_runner = self.getRunner()
#         self.method_1 = "module_name.function_name"
#         self.job_params_1 = get_sample_job_params(method=self.method_1, wsid=self.ws_id)
#
#         # TODO
#         # PATCH OUT LOGIN/WORKSPACE HERE
#         # self.good_job_id_user1 = setup_runner.run_job(params=job_params_1,as_admin=False)
#         # self.good_job_id_user2 = setup_runner.run_job(params=job_params_1,as_admin=False)
#
#     def tearDown(self) -> None:
#         self.catalog_patch.stop()
#         self.condor_patch.stop()
#
#     def getRunner(self) -> SDKMethodRunner:
#         # Initialize these clients from None
#         runner = copy.deepcopy(self.__class__.method_runner)  # type : SDKMethodRunner
#         runner.get_jobs_status()
#         runner.get_runjob()
#         runner.get_job_logs()
#
#         return runner
#
#     def get_runner_with_condor(self) -> SDKMethodRunner:
#         runner = self.getRunner()
#         condor = MagicMock(return_value={})
#         condor.get_job_info = MagicMock(return_value="")
#         condor.get_job_resource_info = MagicMock(return_value="njs")
#         condor.extract_resources = MagicMock(return_value=self.cr)
#         runner.condor = condor
#         runner.catalog_utils.catalog.get_client_groups = MagicMock(return_value=["njs"])
#
#         return runner
#
#     # TODO How do you test ADMIN_MODE without increasing too much coverage
#
#     @patch.object(AdminAuthUtil, "_fetch_user_roles")
#     @patch.object(WorkspaceAuth, "can_write", return_value=True)
#     @patch.object(Catalog, "get_module_version", return_value="module.version")
#     def test_regular_user(self, aau, workspace, catalog):
#         # Regular User
#         lowly_user = "Access Denied: You are not an administrator"
#
#         aau.return_value = ["RegularJoe"]
#         method_1 = "module_name.function_name"
#         job_params_1 = get_sample_job_params(method=method_1, wsid=self.ws_id)
#         runner = self.get_runner_with_condor()
#
#         # Check Admin Status
#         is_admin = runner.check_is_admin()
#         self.assertFalse(is_admin)
#
#         # Check Admin Status
#         admin_type = runner.get_admin_permission()
#         self.assertEqual(admin_type, {"permission": "n"})
#
#         # RUNJOB
#         job_id = runner.run_job(params=job_params_1, as_admin=False)
#         self.assertTrue(bson.objectid.ObjectId.is_valid(job_id))
#
#         # RUNJOB BUT ATTEMPT TO BE AN ADMIN
#         with self.assertRaisesRegexp(
#             expected_exception=PermissionError, expected_regex=lowly_user
#         ):
#             runner.run_job(params=job_params_1, as_admin=True)
#
#         # get_job_params
#         params = runner.get_job_params(job_id=job_id)
#         self.assertEqual(params["method"], job_params_1["method"])
#
#         # get_job_params BUT ATTEMPT TO BE AN ADMIN
#         with self.assertRaisesRegexp(
#             expected_exception=PermissionError, expected_regex=lowly_user
#         ):
#             runner.get_job_params(job_id=job_id, as_admin=True)
#
#         # LOGS #
#         # add_job_logs and view them
#         lines = []
#         for item in ["this", "is", "a", "test"]:
#             line = {"error": False, "line": item}
#             lines.append(line)
#
#         runner.add_job_logs(job_id=job_id, log_lines=lines)
#         runner.view_job_logs(job_id=job_id)
#
#         # add_job_logs and view them, BUT ATTEMPT TO BE AN ADMIN
#         with self.assertRaisesRegexp(
#             expected_exception=PermissionError, expected_regex=lowly_user
#         ):
#             runner.add_job_logs(job_id=job_id, log_lines=lines, as_admin=True)
#
#         with self.assertRaisesRegexp(
#             expected_exception=PermissionError, expected_regex=lowly_user
#         ):
#             runner.view_job_logs(job_id=job_id, as_admin=True)
#
#         # Start the job and get it's status
#         runner.start_job(job_id=job_id)
#         status_field = runner.get_job_status_field(job_id=job_id)
#         self.assertEqual(status_field["status"], Status.running.value)
#         runner.finish_job(job_id=job_id, error_message="Fail")
#         check_job = runner.check_job(job_id=job_id)
#         self.assertEqual(check_job["status"], Status.error.value)
#         job_id2 = runner.run_job(params=job_params_1, as_admin=False)
#         self.assertTrue(bson.objectid.ObjectId.is_valid(job_id2))
#         runner.cancel_job(job_id=job_id2)
#         check_job2 = runner.check_job(job_id=job_id2)
#         self.assertEqual(check_job2["status"], Status.terminated.value)
#
#         # TODO do the above with as_admin=True and assert failure each time
#
#         # Start the job and get it's status as an admin
#
#     @patch.object(Catalog, "get_module_version", return_value="module.version")
#     # @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
#     @patch.object(AdminAuthUtil, "_fetch_user_roles")
#     def test_admin_writer(self, aau,  catalog):
#
#         # Admin User with WRITE
#         # SET YOUR ADMIN STATUS HERE
#
#         method_1 = "module_name.function_name"
#         job_params_1 = get_sample_job_params(method=method_1, wsid=self.ws_id)
#         runner = self.get_runner_with_condor()
#
#         aau.return_value = [self.ADMIN_WRITE_ROLE]
#
#         # Check Admin Status
#         is_admin = runner.check_is_admin()
#         self.assertTrue(is_admin)
#
#         # Check Admin Status
#         admin_type = runner.get_admin_permission()
#         self.assertEqual(admin_type, {"permission": "w"})
#
#         # RUNJOB
#         job_id = runner.run_job(params=job_params_1, as_admin=True)
#         self.assertTrue(bson.objectid.ObjectId.is_valid(job_id))
#
#         # CHECKJOB
#         check_job = runner.check_job(job_id=job_id, as_admin=True)
#         self.assertEqual(check_job.get("status"), Status.queued.value)
#
#         # get_job_params
#         params = runner.get_job_params(job_id=job_id, as_admin=True)
#         self.assertEqual(params["method"], job_params_1["method"])
#
#         # runner.handle_held_job(cluster_id=check_job.get("scheduler_id"))
#
#     # These tests should throw the most errors
#
#     def test_no_user(self):
#         # No Token
#         runner = self.getRunner()
#         method_1 = "module_name.function_name"
#         job_params_1 = get_sample_job_params(method=method_1, wsid=self.ws_id)
#
#         with self.assertRaisesRegex(
#             expected_exception=RuntimeError,
#             expected_regex=r"ServerError\('Token validation failed: Login failed! Server responded with code 401 Unauthorized'\)",
#         ):
#             runner.run_job(params=job_params_1, as_admin=False)
#
#     @patch.object(AdminAuthUtil, "_fetch_user_roles")
#     def test_admin_reader(self, aau):
#         # Admin User with WRITE
#         lowly_admin = r"Access Denied: You are a read-only admin. This function requires write access"
#         runner = self.getRunner()
#         aau.return_value = [runner.ADMIN_READ_ROLE]
#         method_1 = "module_name.function_name"
#         job_params_1 = get_sample_job_params(method=method_1, wsid=self.ws_id)
#         runner = self.getRunner()
#
#         # Check Admin Status
#         is_admin = runner.check_is_admin()
#         self.assertTrue(is_admin)
#
#         # Check Admin Status
#         admin_type = runner.get_admin_permission()
#         self.assertEqual(admin_type, {"permission": "r"})
#
#         # RUNJOB
#         with self.assertRaisesRegexp(
#             expected_exception=PermissionError, expected_regex=lowly_admin
#         ):
#             runner.run_job(params=job_params_1, as_admin=True)
#         #
#         # good_job_id = 1
#         # check_job = runner.check_job(job_id=good_job_id)
#         # self.assertEqual(check_job.get("status"),Status.queued.value)
