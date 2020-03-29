# -*- coding: utf-8 -*-
import copy
import logging
import os
import unittest
from configparser import ConfigParser
from typing import Dict, List

import requests_mock

from sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.db.models.models import Job, JobLog
from test.ee2_SDKMethodRunner_test_utils import ee2_sdkmr_test_helper
from test.mongo_test_helper import MongoTestHelper
from test.utils.test_utils import bootstrap

logging.basicConfig(level=logging.INFO)
bootstrap()


def _run_job_adapter(
    ws_perms_info: Dict = None,
    ws_perms_global: List = [],
    client_groups_info: Dict = None,
    module_versions: Dict = None,
    user_roles: List = None,
):
    """
    Mocks POST calls to:
        Workspace.get_permissions_mass,
        Catalog.list_client_group_configs,
        Catalog.get_module_version
    Mocks GET calls to:
        Auth (/api/V2/me)
        Auth (/api/V2/token)

    Returns an Adapter for requests_mock that deals with mocking workspace permissions.
    :param ws_perms_info: dict - keys user_id, and ws_perms
            user_id: str - the user id
            ws_perms: dict of permissions, keys are ws ids, values are permission. Example:
                {123: "a", 456: "w"} means workspace id 123 has admin permissions, and 456 has
                write permission
    :param ws_perms_global: list - list of global workspaces - gives those workspaces a global (user "*") permission of "r"
    :param client_groups_info: dict - keys client_groups (list), function_name, module_name
    :param module_versions: dict - key git_commit_hash (str), others aren't used
    :return: an adapter function to be passed to request_mock
    """


class ee2_SDKMethodRunner_test_ee2_logs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
        config_parser = ConfigParser()
        config_parser.read(config_file)

        cfg = {}

        for nameval in config_parser.items("execution_engine2"):
            cfg[nameval[0]] = nameval[1]
        cls.cfg = cfg
        mongo_in_docker = cls.cfg.get("mongo-in-docker-compose", None)
        if mongo_in_docker is not None:
            cls.cfg["mongo-host"] = cls.cfg["mongo-in-docker-compose"]
        logging.info(f"Mongo host is {cls.cfg['mongo-host']}")
        cls.user_id = "wsadmin"
        cls.ws_id = 9999
        cls.token = "token"

        cls.method_runner = SDKMethodRunner(
            cls.cfg, user_id=cls.user_id, token=cls.token
        )
        cls.mongo_util = MongoUtil(cls.cfg)
        cls.mongo_helper = MongoTestHelper(cls.cfg)

        cls.test_collection = cls.mongo_helper.create_test_db(
            db=cls.cfg["mongo-database"], col=cls.cfg["mongo-jobs-collection"]
        )

        cls.test_helper = ee2_sdkmr_test_helper(cfg)

    def getRunner(self) -> SDKMethodRunner:
        return copy.deepcopy(self.__class__.method_runner)

    @requests_mock.Mocker()
    def test_add_job_logs_ok(self, rq_mock):
        rq_mock.add_matcher(
            _run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}},
                user_roles=[],
            )
        )
        with self.mongo_util.mongo_engine_connection():
            ori_job_log_count = JobLog.objects.count()
            ori_job_count = Job.objects.count()
            job_id = self.test_helper.create_job_rec()
            self.assertEqual(ori_job_count, Job.objects.count() - 1)

            runner = self.getRunner()

            # create new log
            lines = [{"line": "Hello world"}]
            runner.add_job_logs(job_id=job_id, log_lines=lines)

            updated_job_log_count = JobLog.objects.count()
            self.assertEqual(ori_job_log_count, updated_job_log_count - 1)

            log = self.mongo_util.get_job_log(job_id=job_id)
            ori_updated_time = log.updated
            self.assertTrue(ori_updated_time)
            self.assertEqual(log.original_line_count, 1)
            self.assertEqual(log.stored_line_count, 1)
            ori_lines = log.lines
            self.assertEqual(len(ori_lines), 1)

            test_line = ori_lines[0]

            self.assertEqual(test_line["line"], "Hello world")
            self.assertEqual(test_line["linepos"], 1)
            self.assertFalse(test_line["error"])

            # add job log
            lines = [
                {"error": True, "line": "Hello Kbase"},
                {"line": "Hello Wrold Kbase"},
            ]

            runner.add_job_logs(job_id=job_id, log_lines=lines)

            log = self.mongo_util.get_job_log(job_id=job_id)
            self.assertTrue(log.updated)
            self.assertTrue(ori_updated_time < log.updated)
            self.assertEqual(log.original_line_count, 3)
            self.assertEqual(log.stored_line_count, 3)
            ori_lines = log.lines
            self.assertEqual(len(ori_lines), 3)

            # original line
            test_line = ori_lines[0]

            self.assertEqual(test_line["line"], "Hello world")
            self.assertEqual(test_line["linepos"], 1)
            self.assertFalse(test_line["error"])

            # new line
            test_line = ori_lines[1]

            self.assertEqual(test_line["line"], "Hello Kbase")
            self.assertEqual(test_line["linepos"], 2)
            self.assertTrue(test_line["error"])

            test_line = ori_lines[2]

            self.assertEqual(test_line["line"], "Hello Wrold Kbase")
            self.assertEqual(test_line["linepos"], 3)
            self.assertFalse(test_line["error"])

            self.mongo_util.get_job(job_id=job_id).delete()
            self.assertEqual(ori_job_count, Job.objects.count())

            self.mongo_util.get_job_log(job_id=job_id).delete()
            self.assertEqual(ori_job_log_count, JobLog.objects.count())
