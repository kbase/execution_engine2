# -*- coding: utf-8 -*-
import copy
import logging
import os
import unittest
from configparser import ConfigParser
from unittest.mock import patch

import requests_mock
from mock import MagicMock
from mongoengine import ValidationError

from lib.execution_engine2.db.models.models import Job
from lib.execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from lib.execution_engine2.utils.CondorTuples import SubmissionInfo, CondorResources
from test.tests_for_sdkmr.ee2_SDKMethodRunner_test_utils import ee2_sdkmr_test_helper
from test.utils_shared.test_utils import bootstrap, get_example_job

logging.basicConfig(level=logging.INFO)
bootstrap()

from test.utils_shared.test_utils import (
    get_example_job_as_dict_for_runjob,
    run_job_adapter,
)
from lib.execution_engine2.db.models.models import Status


class ee2_SDKMethodRunner_test_status(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
        logging.info(f"Loading config from {config_file}")
        config_parser = ConfigParser()
        config_parser.read(config_file)
        cls.cfg = {}
        for nameval in config_parser.items("execution_engine2"):
            cls.cfg[nameval[0]] = nameval[1]
        mongo_in_docker = cls.cfg.get("mongo-in-docker-compose", None)
        if mongo_in_docker is not None:
            cls.cfg["mongo-host"] = cls.cfg["mongo-in-docker-compose"]
        cls.user_id = "wsadmin"

        cls.ws_id = 9999
        cls.token = "token"

        cls.method_runner = SDKMethodRunner(
            cls.cfg, user_id=cls.user_id, token=cls.token
        )
        cls.cr = CondorResources(
            request_cpus="1",
            request_disk="1GB",
            request_memory="100M",
            client_group="njs",
        )
        cls.fake_used_resources = {
            "RemoteUserCpu": "1",
            "DiskUsage_RAW": "1",
            "DiskUsage": "1",
        }
        cls.mongo_util = cls.method_runner.get_mongo_util()
        cls.sdkmr_test_helper = ee2_sdkmr_test_helper(mr=cls.method_runner)

    def getRunner(self) -> SDKMethodRunner:
        # Initialize these clients from None
        runner = copy.copy(self.method_runner)  # type : SDKMethodRunner
        runner.get_jobs_status()
        runner.get_runjob()
        runner.get_job_logs()
        return runner

    def create_job_rec(self):
        return self.sdkmr_test_helper.create_job_rec()

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_run_job_and_handle_held(self, rq_mock, condor_mock):
        """
        Run a job, then call it held as an admin, and then check to see if the record contains condor info about the job
        :param rq_mock:
        :param condor_mock:
        :return:
        """
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}},
                user_roles=["EE2_ADMIN"],
            )
        )
        runner = self.getRunner()
        runner.get_condor = MagicMock(return_value=condor_mock)
        job = get_example_job_as_dict_for_runjob(user=self.user_id, wsid=self.ws_id)

        si = SubmissionInfo(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)
        condor_mock.extract_resources = MagicMock(return_value=self.cr)
        condor_mock.get_job_resource_info = MagicMock(
            return_value=self.fake_used_resources
        )
        job_id = runner.run_job(params=job)

        check_job = runner.check_job(job_id=job_id)
        print(
            f"Job id is {job_id}. Status is {check_job.get('status')} Cluster is {check_job.get('scheduler_id')} "
        )

        job_record = runner.handle_held_job(cluster_id=check_job.get("scheduler_id"))
        print("Records are", job_record.get("condor_job_ads"))
        self.assertEqual(self.fake_used_resources, job_record.get("condor_job_ads"))

    def test_update_job_status(self):
        runner = self.getRunner()
        mongo_util = runner.get_mongo_util()
        with mongo_util.mongo_engine_connection():
            ori_job_count = Job.objects.count()
            job_id = self.create_job_rec()
            self.assertEqual(ori_job_count, Job.objects.count() - 1)

            runner._test_job_permissions = MagicMock(return_value=True)

            # test missing status
            with self.assertRaises(ValueError) as context:
                runner.update_job_status(None, "invalid_status")
            self.assertEqual(
                "Please provide both job_id and status", str(context.exception)
            )

            # test invalid status
            with self.assertRaises(ValidationError) as context:
                runner.update_job_status(job_id, "invalid_status")
            self.assertIn("is not a valid status", str(context.exception))

            ori_job = Job.objects(id=job_id)[0]
            ori_updated_time = ori_job.updated

            # test update job status
            job_id = runner.update_job_status(job_id, "estimating")
            updated_job = Job.objects(id=job_id)[0]
            self.assertEqual(updated_job.status, "estimating")
            updated_time = updated_job.updated

            self.assertTrue(ori_updated_time < updated_time)

            mongo_util.get_job(job_id=job_id).delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    def test_get_job_status(self):
        runner = self.getRunner()
        mongo_util = runner.get_mongo_util()
        with mongo_util.mongo_engine_connection():
            ori_job_count = Job.objects.count()
            job_id = self.create_job_rec()
            self.assertEqual(ori_job_count, Job.objects.count() - 1)

            runner._test_job_permissions = MagicMock(return_value=True)

            # test missing job_id input
            with self.assertRaises(ValueError) as context:
                runner.get_job_status_field(None)
            self.assertEqual("Please provide valid job_id", str(context.exception))

            returnVal = runner.get_job_status_field(job_id)

            self.assertTrue("status" in returnVal)
            self.assertEqual(returnVal["status"], "created")

            mongo_util.get_job(job_id=job_id).delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_cancel_job_batch(self, rq_mock, condor_mock):
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}}
            )
        )
        runner = self.getRunner()  # type: SDKMethodRunner
        runner.get_condor = MagicMock(return_value=condor_mock)
        job = get_example_job_as_dict_for_runjob(user=self.user_id, wsid=self.ws_id)

        si = SubmissionInfo(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)
        condor_mock.extract_resources = MagicMock(return_value=self.cr)

        jobs = [job, job, job]
        job_ids = runner.run_job_batch(params=jobs, batch_params={"wsid": self.ws_id})
        assert "parent_job_id" in job_ids and isinstance(job_ids["parent_job_id"], str)
        assert "child_job_ids" in job_ids and isinstance(job_ids["child_job_ids"], list)
        assert len(job_ids["child_job_ids"]) == len(jobs)

        runner.cancel_job(job_id=job_ids["parent_job_id"])
        job_status = runner.check_jobs(
            job_ids=[job_ids["parent_job_id"]] + job_ids["child_job_ids"]
        )
        for job in job_status["job_states"]:
            assert job["status"] == Status.terminated.value

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_abandon_children(self, rq_mock, condor_mock):
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}}
            )
        )
        runner = self.getRunner()  # type: SDKMethodRunner
        runner.get_condor = MagicMock(return_value=condor_mock)
        job = get_example_job_as_dict_for_runjob(user=self.user_id, wsid=self.ws_id)

        si = SubmissionInfo(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)
        condor_mock.extract_resources = MagicMock(return_value=self.cr)

        jobs = [job, job, job]
        job_ids = runner.run_job_batch(params=jobs, batch_params={"wsid": self.ws_id})

        assert "parent_job_id" in job_ids and isinstance(job_ids["parent_job_id"], str)
        assert "child_job_ids" in job_ids and isinstance(job_ids["child_job_ids"], list)
        assert len(job_ids["child_job_ids"]) == len(jobs)

        runner.abandon_children(
            parent_job_id=job_ids["parent_job_id"],
            child_job_ids=job_ids["child_job_ids"][0:2],
        )

        job_status = runner.check_jobs(job_ids=[job_ids["parent_job_id"]])[
            "job_states"
        ][0]

        for job_id in job_ids["child_job_ids"][0:2]:
            assert job_id not in job_status["child_jobs"]

        assert job_ids["child_job_ids"][2] in job_status["child_jobs"]

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_check_job_batch(self, rq_mock, condor_mock):
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}}
            )
        )
        runner = self.getRunner()  # type: SDKMethodRunner
        runner.get_condor = MagicMock(return_value=condor_mock)
        job = get_example_job_as_dict_for_runjob(user=self.user_id, wsid=self.ws_id)

        si = SubmissionInfo(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)
        condor_mock.extract_resources = MagicMock(return_value=self.cr)

        jobs = [job, job, job]
        job_ids = runner.run_job_batch(params=jobs, batch_params={"wsid": self.ws_id})

        job_status = runner.check_job_batch(parent_job_id=job_ids["parent_job_id"])
        # parent_job_state = job_status["parent_jobstate"]
        child_jobstates = job_status["child_jobstates"]

        assert len(child_jobstates) == len(jobs)
