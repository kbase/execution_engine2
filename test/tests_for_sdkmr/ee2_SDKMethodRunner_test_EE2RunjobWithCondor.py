# # -*- coding: utf-8 -*-
# import copy
# import logging
# import os
# import unittest
# from configparser import ConfigParser
# from unittest.mock import patch
#
# import requests_mock
# from mock import MagicMock
#
# from lib.execution_engine2.db.MongoUtil import MongoUtil
# from lib.execution_engine2.db.models.models import Job
# from lib.execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
# from lib.execution_engine2.utils.CondorTuples import SubmissionInfo, CondorResources
# from test.utils_shared.test_utils import (
#     bootstrap,
#     get_example_job,
#     run_job_adapter,
#     get_example_job_as_dict,
# )
# from tests_for_db.mongo_test_helper import MongoTestHelper
#
# logging.basicConfig(level=logging.INFO)
# bootstrap()
#
# from test.tests_for_sdkmr.ee2_SDKMethodRunner_test_utils import ee2_sdkmr_test_helper
#
#
# class ee2_SDKMethodRunner_test(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls):
#         config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
#         logging.info(f"Loading config from {config_file}")
#
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
#
#         cls.user_id = 'boris'
#         cls.token = "token"
#         cls.ws_id = 123
#
#         cls.method_runner = SDKMethodRunner(
#             cls.cfg, user_id=cls.user_id, token=cls.token
#         )
#
#     def getRunner(self) -> SDKMethodRunner:
#         # Initialize these clients from None
#         runner = copy.copy(self.__class__.method_runner)  # type : SDKMethodRunner
#         runner.get_jobs_status()
#         runner.get_runjob()
#         runner.get_job_logs()
#         return runner
#
#     def create_job_rec(self):
#         return self.sdkmr_test_helper.create_job_rec()
#
#     def test_init_ok(self):
#         class_attri = ["config", "catalog_utils", "workspace", "mongo_util", "condor"]
#         runner = self.getRunner()
#         self.assertTrue(set(class_attri) <= set(runner.__dict__.keys()))
#
#     @requests_mock.Mocker()
#     @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
#     def test_run_job(self, rq_mock, condor_mock):
#         rq_mock.add_matcher(
#             run_job_adapter(
#                 ws_perms_info={
#                     "user_id": self.user_id,
#                     "ws_perms": {self.ws_id: "a"},
#                 }
#             )
#         )
#         runner = self.getRunner()
#         job = get_example_job_as_dict(user=self.user_id, wsid=self.ws_id)
#         #TODO Mock out WSID Perms
#         #TODO change submission to sleep
#         si = SubmissionInfo(clusterid="test", submit=job, error=None)
#         job_id = runner.run_job(params=job)
#         print(f"Job id is {job_id} ")
#
