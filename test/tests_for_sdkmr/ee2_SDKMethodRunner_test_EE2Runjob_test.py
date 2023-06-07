# -*- coding: utf-8 -*-
import copy
import logging
import os
import unittest
from configparser import ConfigParser
from unittest.mock import patch

import requests_mock
from mock import MagicMock

from execution_engine2.exceptions import (
    CannotRetryJob,
    InvalidParameterForBatch,
)
from execution_engine2.sdk.job_submission_parameters import JobRequirements
from execution_engine2.utils.clients import (
    get_client_set,
    get_user_client_set,
)
from installed_clients.CatalogClient import Catalog
from lib.execution_engine2.db.MongoUtil import MongoUtil
from lib.execution_engine2.db.models.models import Job, Status
from lib.execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from lib.execution_engine2.utils.CondorTuples import SubmissionInfo
from test.tests_for_sdkmr.ee2_SDKMethodRunner_test_utils import ee2_sdkmr_test_helper
from test.utils_shared.test_utils import (
    bootstrap,
    get_example_job,
    run_job_adapter,
    get_example_job_as_dict,
)
from tests_for_db.mongo_test_helper import MongoTestHelper

logging.basicConfig(level=logging.INFO)
bootstrap()


class ee2_SDKMethodRunner_test(unittest.TestCase):
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

        with open(config_file) as cf:
            cls.method_runner = SDKMethodRunner(
                get_user_client_set(cls.cfg, cls.user_id, cls.token),
                get_client_set(cls.cfg, cf),
            )

        cls.mongo_util = MongoUtil(cls.cfg)
        cls.mongo_helper = MongoTestHelper(cls.cfg)

        cls.test_collection = cls.mongo_helper.create_test_db(
            db=cls.cfg["mongo-database"], col=cls.cfg["mongo-jobs-collection"]
        )

        cls.sdkmr_test_helper = ee2_sdkmr_test_helper(cls.user_id)

    def getRunner(self) -> SDKMethodRunner:
        """
        # Initialize these clients from None
        # Set up some mocks
        """
        runner = copy.copy(self.__class__.method_runner)  # type : SDKMethodRunner
        runner.get_jobs_status()
        runner.get_runjob()
        runner.get_job_logs()
        runner.get_workspace()
        runner.workspace.get_object_info3 = MagicMock(return_value={"paths": []})
        return runner

    def create_job_rec(self):
        return self.sdkmr_test_helper.create_job_rec()

    def test_init_ok(self):
        class_attri = ["workspace", "mongo_util", "condor"]
        runner = self.getRunner()
        self.assertTrue(set(class_attri) <= set(runner.__dict__.keys()))

    @patch.object(Catalog, "get_module_version")
    def test_init_job_rec(self, get_mod_ver):
        ori_job_count = Job.objects.count()
        runner = self.getRunner()

        job_params = {
            "wsid": self.ws_id,
            "method": "MEGAHIT.run_megahit",
            "app_id": "MEGAHIT/run_megahit",
            "service_ver": "2.2.1",
            "params": [
                {
                    "workspace_name": "wjriehl:1475006266615",
                    "read_library_refs": ["18836/5/1"],
                    "output_contigset_name": "rhodo_contigs",
                    "recipe": "auto",
                    "assembler": None,
                    "pipeline": None,
                    "min_contig_len": None,
                }
            ],
            "job_reqs": JobRequirements(1, 1, 1, "njs"),
            "source_ws_objects": ["a/b/c", "e/d"],
            "parent_job_id": "9998",
            "meta": {"tag": "dev", "token_id": "12345"},
        }

        get_mod_ver.return_value = {
            "git_commit_hash": "048baf3c2b76cb923b3b4c52008ed77dbe20292d"
        }

        job_id = runner.get_runjob()._init_job_rec(self.user_id, job_params)

        get_mod_ver.assert_called_once_with(
            {"module_name": "MEGAHIT", "version": "2.2.1"}
        )

        self.assertEqual(ori_job_count, Job.objects.count() - 1)

        job = Job.objects.get(id=job_id)

        self.assertEqual(job.user, self.user_id)
        self.assertEqual(job.authstrat, "kbaseworkspace")
        self.assertEqual(job.wsid, self.ws_id)

        job_input = job.job_input

        self.assertEqual(job_input.wsid, self.ws_id)
        self.assertEqual(job_input.method, "MEGAHIT.run_megahit")
        self.assertEqual(job_input.app_id, "MEGAHIT/run_megahit")
        # TODO this is an integration test
        # self.assertEqual(job_input.service_ver, "2.2.1")
        self.assertEqual(
            job_input.service_ver, "048baf3c2b76cb923b3b4c52008ed77dbe20292d"
        )

        self.assertCountEqual(job_input.source_ws_objects, ["a/b/c", "e/d"])
        self.assertEqual(job_input.parent_job_id, "9998")

        narrative_cell_info = job_input.narrative_cell_info
        self.assertEqual(narrative_cell_info.tag, "dev")
        self.assertEqual(narrative_cell_info.token_id, "12345")

        self.assertFalse(job.job_output)

        self.mongo_util.get_job(job_id=job_id).delete()
        self.assertEqual(ori_job_count, Job.objects.count())

    def test_get_job_params(self):

        ori_job_count = Job.objects.count()
        job_id = self.create_job_rec()
        self.assertEqual(ori_job_count, Job.objects.count() - 1)

        runner = self.getRunner()
        runner._test_job_permissions = MagicMock(return_value=True)
        params = runner.get_job_params(job_id)

        expected_params_keys = [
            "wsid",
            "method",
            "params",
            "service_ver",
            "app_id",
            "source_ws_objects",
            "parent_job_id",
        ]
        self.assertCountEqual(params.keys(), expected_params_keys)
        self.assertEqual(params["wsid"], self.ws_id)
        self.assertEqual(params["method"], "MEGAHIT.run_megahit")
        self.assertEqual(params["app_id"], "MEGAHIT/run_megahit")
        self.assertEqual(params["service_ver"], "2.2.1")
        self.assertCountEqual(params["source_ws_objects"], ["a/b/c", "e/d"])
        self.assertEqual(params["parent_job_id"], "9998")

        self.mongo_util.get_job(job_id=job_id).delete()
        self.assertEqual(ori_job_count, Job.objects.count())

    def test_start_job(self):

        ori_job_count = Job.objects.count()
        job_id = self.create_job_rec()
        self.assertEqual(ori_job_count, Job.objects.count() - 1)

        job = self.mongo_util.get_job(job_id=job_id)
        self.assertEqual(job.status, "created")
        self.assertFalse(job.finished)
        self.assertFalse(job.running)
        self.assertFalse(job.estimating)

        runner = self.getRunner()
        runner._test_job_permissions = MagicMock(return_value=True)

        # test missing job_id input
        with self.assertRaises(ValueError) as context:
            runner.start_job(None)
            self.assertEqual("Please provide valid job_id", str(context.exception))

        # start a created job, set job to estimation status
        runner.start_job(job_id, skip_estimation=False)

        job = self.mongo_util.get_job(job_id=job_id)
        self.assertEqual(job.status, "estimating")
        self.assertFalse(job.running)
        self.assertTrue(job.estimating)

        # start a estimating job, set job to running status
        runner.start_job(job_id, skip_estimation=False)

        job = self.mongo_util.get_job(job_id=job_id)
        self.assertEqual(job.status, "running")
        self.assertTrue(job.running)
        self.assertTrue(job.estimating)

        # test start a job with invalid status
        with self.assertRaises(ValueError) as context:
            runner.start_job(job_id)
        self.assertIn("Unexpected job status", str(context.exception))

        self.mongo_util.get_job(job_id=job_id).delete()
        self.assertEqual(ori_job_count, Job.objects.count())

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_run_job(self, rq_mock, condor_mock):
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}}
            )
        )
        runner = self.getRunner()
        runner.get_condor = MagicMock(return_value=condor_mock)
        job = get_example_job_as_dict(user=self.user_id, wsid=self.ws_id)

        si = SubmissionInfo(clusterid="test", submit=job, error=None)

        # OK
        condor_mock.run_job = MagicMock(return_value=si)
        runner.run_job(params=job)

        # Condor Failure Case Coverage
        condor_mock.run_job = MagicMock(return_value=si, side_effect=Exception("fail"))
        runner.get_runjob()._finish_created_job = MagicMock(return_value=None)

        with self.assertRaises(expected_exception=Exception):
            runner.run_job(params=job)

        # Condor Failure Case Coverage #2
        with self.assertRaisesRegex(
            expected_exception=RuntimeError,
            expected_regex="Condor job not run, and error not found. Something went wrong",
        ):
            si = SubmissionInfo(clusterid=None, submit=job, error=None)
            condor_mock.run_job = MagicMock(return_value=si)
            runner.run_job(params=job)

    @staticmethod
    def check_retry_job_state(job_id: str, retry_job_id: str):
        """
        Checks to see the required keys are there
        :param job_id: The job that was retried
        :param retry_job_id: The job id that was a result of the retry

        """
        job = Job.objects.get(id=job_id)  # type: Job
        retry_job = Job.objects.get(id=retry_job_id)  # type: Job

        check_attributes = [
            "job_input",
            "wsid",
            "authstrat",
            "batch_job",
            "batch_id",
            "scheduler_type",
        ]

        for item in check_attributes:
            if job[item]:
                assert job[item] == retry_job[item]

        assert retry_job.retry_parent == job_id
        assert len(job.retry_ids) > 0
        assert retry_job_id in job.retry_ids
        assert not job.retry_saved_toggle and retry_job.retry_saved_toggle

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_retry_job_multiple(self, rq_mock, condor_mock):
        # 1. Run the job
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}}
            )
        )
        runner = self.getRunner()
        runner.get_condor = MagicMock(return_value=condor_mock)

        job = get_example_job_as_dict(
            user=self.user_id, wsid=self.ws_id, source_ws_objects=[]
        )
        si = SubmissionInfo(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)

        parent_job_id0 = runner.run_job(params=job)
        parent_job_id1 = runner.run_job(params=job)
        parent_job_id2 = runner.run_job(params=job)
        parent_job_id3 = runner.run_job(params=job)
        parent_job_id4 = runner.run_job(params=job)

        runner.update_job_status(job_id=parent_job_id0, status=Status.terminated.value)
        runner.update_job_status(job_id=parent_job_id1, status=Status.terminated.value)
        runner.update_job_status(job_id=parent_job_id2, status=Status.error.value)
        runner.update_job_status(job_id=parent_job_id3, status=Status.terminated.value)
        runner.update_job_status(job_id=parent_job_id4, status=Status.error.value)

        # 2. Retry the jobs with a fake input
        errmsg = (
            "'123' is not a valid ObjectId, it must be a 12-byte input or a 24-character "
            "hex string"
        )
        errmsg2 = (
            "'1234' is not a valid ObjectId, it must be a 12-byte input or a 24-character "
            "hex string"
        )
        retry_results = runner.retry_multiple(job_ids=[1234, 123, parent_job_id0])
        assert retry_results[0] == {"job_id": 1234, "error": errmsg2}
        assert retry_results[1] == {"job_id": 123, "error": errmsg}
        assert retry_results[2]["job_id"] == parent_job_id0

        # 3. Retry the jobs with duplicate job ids
        retry_candidates = (
            parent_job_id1,
            parent_job_id2,
            parent_job_id1,
            parent_job_id2,
        )
        fail_msg = f"Retry of the same id in the same request is not supported. Offending ids: {[parent_job_id1, parent_job_id2]} "

        with self.assertRaises(ValueError) as e:
            runner.retry_multiple(retry_candidates)
        assert str(e.exception) == str(ValueError(fail_msg))

        # 4. Retry the jobs
        retry_candidates = (
            parent_job_id1,
            parent_job_id2,
            parent_job_id3,
            parent_job_id4,
        )
        check_job = runner.check_job(parent_job_id1)
        assert check_job["retry_ids"] == []
        assert check_job["retry_count"] == 0
        retry_job_ids = runner.retry_multiple(retry_candidates)

        assert len(retry_job_ids) == len(retry_candidates)

        # Lets retry the jobs a few times
        js = runner.check_jobs(
            job_ids=[
                retry_job_ids[0]["retry_id"],
                retry_job_ids[1]["retry_id"],
                retry_job_ids[2]["retry_id"],
                retry_job_ids[3]["retry_id"],
            ]
        )["job_states"]

        job1, job2, job3, job4 = js

        self.check_retry_job_state(parent_job_id1, job1["job_id"])
        self.check_retry_job_state(parent_job_id2, job2["job_id"])
        self.check_retry_job_state(parent_job_id3, job3["job_id"])
        self.check_retry_job_state(parent_job_id4, job4["job_id"])

        # Test no job ids
        with self.assertRaisesRegex(ValueError, "No job_ids provided to retry"):
            runner.retry_multiple(job_ids=None)

        # Test error during retry, but passing validate
        runner._ee2_runjob._retry = MagicMock(
            side_effect=Exception("Job Retry Misbehaved!")
        )
        misbehaving_jobs = runner.retry_multiple(retry_candidates)
        for i, candidate in enumerate(retry_candidates):
            assert misbehaving_jobs[i] == {
                "error": "Job Retry Misbehaved!",
                "job_id": candidate,
            }

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_retry_job(self, rq_mock, condor_mock):
        # 1. Run the job
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}}
            )
        )
        runner = self.getRunner()
        runner.get_condor = MagicMock(return_value=condor_mock)

        job = get_example_job_as_dict(
            user=self.user_id, wsid=self.ws_id, source_ws_objects=[]
        )
        si = SubmissionInfo(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)
        parent_job_id = runner.run_job(params=job)

        # 2a. Retry the job and fail because it's in progress
        expected_error = f"Error retrying job {parent_job_id} with status running: can only retry jobs with status 'error' or 'terminated'"
        with self.assertRaisesRegex(CannotRetryJob, expected_regex=expected_error):
            runner.update_job_status(job_id=parent_job_id, status=Status.running.value)
            runner.retry(job_id=parent_job_id)

        # 2b. Retry the job
        runner.update_job_status(job_id=parent_job_id, status=Status.terminated.value)
        retry_job_id = runner.retry(job_id=parent_job_id)["retry_id"]

        # 3. Attempt to retry a retry, and check to see that that the new job is retried off of the parent
        runner.update_job_status(job_id=retry_job_id, status=Status.terminated.value)
        retry_from_retry_id = runner.retry(job_id=retry_job_id)["retry_id"]

        retry_from_original_again = runner.retry(job_id=parent_job_id)["retry_id"]
        original_job, retried_job, retried_job2, retried_job3 = runner.check_jobs(
            job_ids=[
                parent_job_id,
                retry_job_id,
                retry_from_retry_id,
                retry_from_original_again,
            ]
        )["job_states"]

        self.check_retry_job_state(parent_job_id, retry_job_id)
        self.check_retry_job_state(parent_job_id, retry_from_retry_id)
        self.check_retry_job_state(parent_job_id, retry_from_original_again)

        for job in [original_job, retried_job, retried_job2, retried_job3]:
            j = Job.objects.get(id=job["job_id"])
            if job == original_job:
                assert original_job["retry_count"] == 3
                assert not j.retry_saved_toggle
            else:
                assert job["retry_parent"] == parent_job_id
                assert j.retry_saved_toggle

        assert [
            retried_job["job_id"],
            retried_job2["job_id"],
            retried_job3["job_id"],
        ] == original_job["retry_ids"]

        # 4. Get jobs and ensure they contain the same keys and params
        same_keys = ["user", "authstrat", "wsid", "scheduler_type", "job_input"]

        assert "retry_parent" not in original_job

        for key in same_keys:
            assert original_job[key] == retried_job[key]

        assert original_job["job_input"]["params"] == retried_job["job_input"]["params"]

        # Some failure cases

        # TODO Retry a job that uses run_job_batch or kbparallels (Like metabat)
        # TODO Retry a job without an app_id

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_retry_job_with_params_and_nci_and_src_ws_objs(self, rq_mock, condor_mock):
        # 1. Run the job
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}}
            )
        )
        runner = self.getRunner()

        runner.get_condor = MagicMock(return_value=condor_mock)

        quast_params = {
            "workspace_name": "XX:narrative_1620418248793",
            "assemblies": ["62160/9/18"],
            "force_glimmer": 0,
        }
        source_ws_objects = quast_params["assemblies"]
        nci = {
            "run_id": "3a211c4e-5ba8-4b94-aeae-378079ccc63d",
            "token_id": "f38f09f7-5ab1-4bfc-9f3f-2b82c7a8dbdc",
            "tag": "release",
            "cell_id": "3ee13d64-623b-407f-98a1-72e577662132",
        }

        job = get_example_job_as_dict(
            user=self.user_id,
            wsid=self.ws_id,
            narrative_cell_info=nci,
            params=quast_params,
            source_ws_objects=source_ws_objects,
            method_name="kb_quast.run_QUAST_app",
            app_id="kb_quast/run_QUAST_app",
        )
        si = SubmissionInfo(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)
        parent_job_id = runner.run_job(params=job)

        # 2. Retry the job
        runner.update_job_status(job_id=parent_job_id, status=Status.terminated.value)
        retry_job_id = runner.retry(job_id=parent_job_id)["retry_id"]

        # 3. Get both jobs and compare them!
        original_job, retried_job = runner.check_jobs(
            job_ids=[parent_job_id, retry_job_id]
        )["job_states"]

        same_keys = ["user", "authstrat", "wsid", "scheduler_type", "job_input"]
        assert "retry_parent" not in original_job
        assert original_job["retry_count"] == 1
        assert retried_job["retry_parent"] == parent_job_id

        for key in same_keys:
            assert original_job[key] == retried_job[key]

        # TODO Possible test additions Retry a job that uses run_job_batch or kbparallels (Like metabat)
        # TODO Retry a job without an app_id
        # TODO Check narrative_cell_info

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_run_job_batch(self, rq_mock, condor_mock):
        """
        Test running batch jobs
        """
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}}
            )
        )
        runner = self.getRunner()
        runner.get_condor = MagicMock(return_value=condor_mock)

        job = get_example_job_as_dict(
            user=self.user_id, wsid=None, source_ws_objects=[]
        )
        job2 = get_example_job_as_dict(
            user=self.user_id, wsid=None, source_ws_objects=[]
        )
        job3 = get_example_job_as_dict(
            user=self.user_id, wsid=None, source_ws_objects=[]
        )
        si = SubmissionInfo(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)
        jobs = [job, job2, job3]
        job_ids = runner.run_job_batch(
            params=copy.deepcopy(jobs), batch_params={"wsid": self.ws_id}
        )

        for job in runner.check_jobs(
            job_ids=job_ids["child_job_ids"] + [job_ids["batch_id"]]
        )["job_states"]:
            assert job.get("wsid") == self.ws_id
            # Job input is forced to assume the batch wsid
            if job["job_id"] != job_ids["batch_id"]:
                assert job.get("job_input", {}).get("wsid") == self.ws_id

        assert "batch_id" in job_ids and isinstance(job_ids["batch_id"], str)
        assert "child_job_ids" in job_ids and isinstance(job_ids["child_job_ids"], list)
        assert len(job_ids["child_job_ids"]) == len(jobs)

        with self.assertRaises(InvalidParameterForBatch):
            job_good = get_example_job_as_dict(
                user=self.user_id, wsid=None, source_ws_objects=[]
            )
            job_bad = (
                get_example_job(user=self.user_id, wsid=self.ws_id).to_mongo().to_dict()
            )
            jobs = [job_good, job_bad]
            runner.run_job_batch(params=jobs, batch_params={"wsid": self.ws_id})

        # Test that you can't run a job in someone elses workspace
        no_perms_ws = 111970
        with self.assertRaises(PermissionError):
            job_good = get_example_job_as_dict(
                user=self.user_id, wsid=None, source_ws_objects=[]
            )
            job_bad = get_example_job(user=self.user_id, wsid=None).to_mongo().to_dict()
            jobs = [job_good, job_bad]
            runner.run_job_batch(params=jobs, batch_params={"wsid": no_perms_ws})

        # Check wsids
        batch_id = job_ids["batch_id"]
        child_job_id = job_ids["child_job_ids"][0]

        # Squeeze in a retry test here
        runner.update_job_status(job_id=child_job_id, status=Status.terminated.value)
        batch_job = runner.check_job(job_id=batch_id)
        assert len(batch_job["child_jobs"]) == 3

        retry_result = runner.retry(job_id=child_job_id)
        retry_id = retry_result["retry_id"]
        self.check_retry_job_state(child_job_id, retry_id)
        batch_job = runner.check_job(job_id=batch_id)
        assert len(batch_job["child_jobs"]) == 4
        assert batch_job["child_jobs"][-1] == retry_id

        job = runner.check_job(job_id=child_job_id)
        retry_count = job["retry_count"]

        # Test to see if one input fails, so keep going
        results = runner.retry_multiple(job_ids=[child_job_id, "grail", "fail"])
        assert results[0]["job_id"] == child_job_id
        assert "error" in results[1]
        assert "error" in results[2]

        # Check to see child_job_id was retried
        assert retry_count + 1 == runner.check_job(job_id=child_job_id)["retry_count"]

        # Test for duplicates
        with self.assertRaises(expected_exception=ValueError) as e:
            runner.retry_multiple(job_ids=[1, 2, 2])
        assert (
            e.exception.args[0]
            == "Retry of the same id in the same request is not supported. Offending ids: [2] "
        )

    @requests_mock.Mocker()
    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_run_job_fail(self, rq_mock, condor_mock):
        rq_mock.add_matcher(
            run_job_adapter(
                ws_perms_info={"user_id": self.user_id, "ws_perms": {self.ws_id: "a"}}
            )
        )
        runner = self.getRunner()

        job = get_example_job_as_dict(user=self.user_id, wsid=self.ws_id)

        si = SubmissionInfo(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)

        with self.assertRaises(expected_exception=RuntimeError):
            runner.run_job(params=job)
