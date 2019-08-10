# -*- coding: utf-8 -*-
import copy
import logging
import os
import unittest
from configparser import ConfigParser
from unittest.mock import patch
from mongoengine import ValidationError
from mock import MagicMock
from bson import ObjectId

from execution_engine2.utils.Condor import submission_info
from execution_engine2.utils.MongoUtil import MongoUtil
from execution_engine2.utils.SDKMethodRunner import SDKMethodRunner
from execution_engine2.models.models import Job, JobInput, Meta, Status
from test.mongo_test_helper import MongoTestHelper
from test.test_utils import bootstrap, get_example_job

logging.basicConfig(level=logging.INFO)
bootstrap()


class SDKMethodRunner_test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
        config_parser = ConfigParser()
        config_parser.read(config_file)

        cls.cfg = {}
        for nameval in config_parser.items("execution_engine2"):
            cls.cfg[nameval[0]] = nameval[1]

        mongo_in_docker = cls.cfg.get("mongo-in-docker-compose", None)
        if mongo_in_docker is not None:
            cls.cfg["mongo-host"] = cls.cfg["mongo-in-docker-compose"]

        cls.method_runner = SDKMethodRunner(cls.cfg)
        cls.mongo_util = MongoUtil(cls.cfg)
        cls.mongo_helper = MongoTestHelper(cls.cfg)

        cls.test_collection = cls.mongo_helper.create_test_db(
            db=cls.cfg["mongo-database"], col=cls.cfg["mongo-jobs-collection"]
        )

        cls.user_id = "fake_test_user"
        cls.ws_id = 9999
        cls.token = "token"

    def getRunner(self) -> SDKMethodRunner:
        return copy.deepcopy(self.__class__.method_runner)

    def create_job_rec(self):
        job = Job()

        inputs = JobInput()

        job.user = "tgu2"
        job.authstrat = "kbaseworkspace"
        job.wsid = 9999
        job.status = "created"

        job_params = {
            "wsid": self.ws_id,
            "method": "MEGAHIT.run_megahit",
            "app_id": "MEGAHIT/run_megahit",
            "service_ver": "2.2.1",
            "params": [
                {
                    "k_list": [],
                    "k_max": None,
                    "output_contigset_name": "MEGAHIT.contigs",
                }
            ],
            "source_ws_objects": ["a/b/c", "e/d"],
            "parent_job_id": "9998",
        }

        inputs.wsid = job.wsid
        inputs.method = job_params.get("method")
        inputs.params = job_params.get("params")
        inputs.service_ver = job_params.get("service_ver")
        inputs.app_id = job_params.get("app_id")
        inputs.source_ws_objects = job_params.get("source_ws_objects")
        inputs.parent_job_id = job_params.get("parent_job_id")

        inputs.narrative_cell_info = Meta()

        job.job_input = inputs

        with self.mongo_util.me_collection(self.cfg["mongo-jobs-collection"]):
            job.save()

        return str(job.id)

    def test_init_ok(self):
        class_attri = ["config", "catalog", "workspace", "mongo_util", "condor"]
        runner = self.getRunner()
        self.assertTrue(set(class_attri) <= set(runner.__dict__.keys()))

    def test_get_client_groups(self):
        runner = self.getRunner()

        client_groups = runner._get_client_groups(
            "kb_uploadmethods.import_sra_from_staging"
        )

        expected_groups = "kb_upload"  # expected to fail if CI catalog is updated
        self.assertCountEqual(expected_groups, client_groups)
        client_groups = runner._get_client_groups("MEGAHIT.run_megahit")
        self.assertEqual(0, len(client_groups))

        with self.assertRaises(ValueError) as context:
            runner._get_client_groups("kb_uploadmethods")

        self.assertIn("unrecognized method:", str(context.exception.args))

    # def test_check_ws_objects(self):
    #     runner = self.getRunner()
    #
    #     [info1, info2] = self.foft.create_fake_reads(
    #         {"ws_name": self.wsName, "obj_names": ["reads1", "reads2"]}
    #     )
    #     read1ref = str(info1[6]) + "/" + str(info1[0]) + "/" + str(info1[4])
    #     read2ref = str(info2[6]) + "/" + str(info2[0]) + "/" + str(info2[4])
    #
    #     runner._check_ws_objects([read1ref, read2ref])
    #
    #     fake_read1ref = str(info1[6]) + "/" + str(info1[0]) + "/" + str(info1[4] + 100)
    #
    #     with self.assertRaises(ValueError) as context:
    #         runner._check_ws_objects([read1ref, read2ref, fake_read1ref])
    #
    #     self.assertIn(
    #         "Some workspace object is inaccessible", str(context.exception.args)
    #     )

    def test_get_module_git_commit(self):

        runner = self.getRunner()

        git_commit_1 = runner._get_module_git_commit("MEGAHIT.run_megahit", "2.2.1")
        self.assertEqual(
            "048baf3c2b76cb923b3b4c52008ed77dbe20292d", git_commit_1
        )  # TODO: works only in CI

        git_commit_2 = runner._get_module_git_commit("MEGAHIT.run_megahit")
        self.assertTrue(isinstance(git_commit_2, str))
        self.assertEqual(len(git_commit_1), len(git_commit_2))
        self.assertNotEqual(git_commit_1, git_commit_2)

    def test_init_job_rec(self):
        with self.mongo_util.me_collection(self.cfg["mongo-jobs-collection"]):
            ori_job_count = Job.objects.count()
            runner = self.getRunner()

            job_params = {
                "wsid": self.ws_id,
                "method": "MEGAHIT.run_megahit",
                "app_id": "MEGAHIT/run_megahit",
                "service_ver": "2.2.1",
                "params": [
                    {
                        "k_list": [],
                        "k_max": None,
                        "output_contigset_name": "MEGAHIT.contigs",
                    }
                ],
                "source_ws_objects": ["a/b/c", "e/d"],
                "parent_job_id": "9998"
            }

            job_id = runner._init_job_rec(self.user_id, job_params)

            self.assertEqual(ori_job_count, Job.objects.count() - 1)

            job = Job.objects.get(id=job_id)

            self.assertEqual(job.user, self.user_id)
            self.assertEqual(job.authstrat, "kbaseworkspace")
            self.assertEqual(job.wsid, self.ws_id)

            job_input = job.job_input

            self.assertEqual(job_input.wsid, self.ws_id)
            self.assertEqual(job_input.method, "MEGAHIT.run_megahit")
            self.assertEqual(job_input.app_id, "MEGAHIT/run_megahit")
            self.assertEqual(job_input.service_ver, "2.2.1")
            self.assertCountEqual(job_input.source_ws_objects, ["a/b/c", "e/d"])
            self.assertEqual(job_input.parent_job_id, "9998")

            self.assertFalse(job.job_output)

            Job.objects.get(id=job_id).delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    @patch("execution_engine2.utils.SDKMethodRunner.SDKMethodRunner", autospec=True)
    def test_cancel_job(self, runner):
        logging.info("\n\n  Test cancel job")
        sdk = copy.deepcopy(self.getRunner())

        with sdk.get_mongo_util().mongo_engine_connection():
            job = get_example_job()
            job.user = self.user_id
            job.wsid = self.ws_id
            job_id = job.save().id

        logging.info(
            f"Created job {job_id} in {job.wsid} status {job.status}. About to cancel"
        )

        sdk.check_permission_for_job = MagicMock(return_value=[])
        sdk.cancel_job(job_id=job_id, ctx={"user_id": self.user_id})

        self.assertEqual(
            Status(sdk.get_mongo_util().get_job(job_id=job_id).status),
            Status.terminated,
        )

    def test_check_ws_permissions(self):
        logging.info("\n\nTESTING PERMISSIONS\n\n")
        sdk = self.getRunner()

        # Check for read access
        for item in [
            sdk.WorkspacePermissions.READ_WRITE,
            sdk.WorkspacePermissions.READ,
            sdk.WorkspacePermissions.ADMINISTRATOR,
        ]:
            self.assertTrue(sdk._can_read_ws(item))

        for item in [sdk.WorkspacePermissions.NONE]:
            self.assertFalse(sdk._can_read_ws(item))

        # Check for write access
        for item in [
            sdk.WorkspacePermissions.READ_WRITE,
            sdk.WorkspacePermissions.ADMINISTRATOR,
        ]:
            self.assertTrue(sdk._can_write_ws(item))

        for item in [sdk.WorkspacePermissions.NONE, sdk.WorkspacePermissions.READ]:
            self.assertFalse(sdk._can_write_ws(item))

    @patch("execution_engine2.utils.MongoUtil.MongoUtil", autospec=True)
    def test_check_job_canceled(self, mongo_util):
        def generateJob(job_id):
            j = Job()
            j.status = job_id
            return j

        runner = self.getRunner()
        runner.get_mongo_util = MagicMock(return_value=mongo_util)
        mongo_util.get_job = MagicMock(side_effect=generateJob)

        call_count = 0
        rv = runner.check_job_canceled("created", {})
        self.assertFalse(rv["canceled"])
        self.assertFalse(rv["finished"])
        call_count += 1

        rv = runner.check_job_canceled("estimating", {})
        self.assertFalse(rv["canceled"])
        self.assertFalse(rv["finished"])
        call_count += 1

        rv = runner.check_job_canceled("queued", {})
        self.assertFalse(rv["canceled"])
        self.assertFalse(rv["finished"])
        call_count += 1

        rv = runner.check_job_canceled("running", {})
        self.assertFalse(rv["canceled"])
        self.assertFalse(rv["finished"])
        call_count += 1

        rv = runner.check_job_canceled("finished", {})
        self.assertFalse(rv["canceled"])
        self.assertTrue(rv["finished"])
        call_count += 1

        rv = runner.check_job_canceled("error", {})
        self.assertFalse(rv["canceled"])
        self.assertTrue(rv["finished"])
        call_count += 1

        rv = runner.check_job_canceled("terminated", {})
        self.assertTrue(rv["canceled"])
        self.assertTrue(rv["finished"])
        call_count += 1

        self.assertEqual(call_count, mongo_util.get_job.call_count)
        self.assertEqual(call_count, runner.get_mongo_util.call_count)

    @patch("lib.installed_clients.WorkspaceClient.Workspace", autospec=True)
    def todo_test_permissions(self, ws):
        runner = self.getRunner()
        runner.get_workspace = MagicMock()
        runner.get_workspace = MagicMock(return_value=ws)
        ws.get_permissions_mass = MagicMock(
            return_value={"perms": [runner.WorkspacePermissions.ADMINISTRATOR]}
        )

    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_run_job(self, condor_mock):
        runner = self.getRunner()
        runner.get_permissions_for_workspace = MagicMock(return_value=True)
        runner._get_module_git_commit = MagicMock(return_value="git_commit_goes_here")
        runner.get_condor = MagicMock(return_value=condor_mock)
        ctx = {"user_id": self.user_id, "wsid": self.ws_id, "token": self.token}
        job = get_example_job().to_mongo().to_dict()
        job["method"] = job["job_input"]["app_id"]
        job["app_id"] = job["job_input"]["app_id"]

        si = submission_info(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)

        job_id = runner.run_job(params=job, ctx=ctx)
        print(f"Job id is {job_id} ")

    @patch("lib.execution_engine2.utils.Condor.Condor", autospec=True)
    def test_run_job_and_add_log(self, condor_mock):
        """
        This test runs a job and then adds logs

        :param condor_mock:
        :return:
        """
        runner = self.getRunner()
        runner.get_permissions_for_workspace = MagicMock(return_value=True)
        runner.check_permission_for_job = MagicMock(return_value=True)

        runner._get_module_git_commit = MagicMock(return_value="git_commit_goes_here")
        runner.get_condor = MagicMock(return_value=condor_mock)
        ctx = {"user_id": self.user_id, "wsid": self.ws_id, "token": self.token}
        job = get_example_job().to_mongo().to_dict()
        job["method"] = job["job_input"]["app_id"]
        job["app_id"] = job["job_input"]["app_id"]

        si = submission_info(clusterid="test", submit=job, error=None)
        condor_mock.run_job = MagicMock(return_value=si)

        job_id = runner.run_job(params=job, ctx=ctx)
        logging.info(f"Job id is {job_id} ")

        lines = []
        for item in ["this", "is", "a", "line"]:
            line = {"error": False, "line": item}
            lines.append(line)

        logging.info("About to insert")
        lp = runner.add_job_logs(ctx=ctx, job_id=job_id, lines=lines)
        logging.info(f"Log position is now {lp}")
        logging.info(runner.view_job_logs(job_id=job_id, skip_lines=None, ctx=ctx))

        self.test_collection.delete_one({"_id": ObjectId(job_id)})
        # TODO FIX THIS ASSERT AS DOCUMENT COUNT IS TOO FLAKY
        # self.assertEqual(self.test_collection.count_documents({}), 0)

    def test_get_job_params(self):

        with self.mongo_util.me_collection(self.cfg["mongo-jobs-collection"]):
            ori_job_count = Job.objects.count()
            job_id = self.create_job_rec()
            self.assertEqual(ori_job_count, Job.objects.count() - 1)

            runner = self.getRunner()
            params = runner.get_job_params(job_id)

            expected_params_keys = ["wsid", "method", "params", "service_ver", "app_id",
                                    "source_ws_objects", "parent_job_id"]
            self.assertCountEqual(params.keys(), expected_params_keys)
            self.assertEqual(params["wsid"], self.ws_id)
            self.assertEqual(params["method"], "MEGAHIT.run_megahit")
            self.assertEqual(params["app_id"], "MEGAHIT/run_megahit")
            self.assertEqual(params["service_ver"], "2.2.1")
            self.assertCountEqual(params["source_ws_objects"], ["a/b/c", "e/d"])
            self.assertEqual(params["parent_job_id"], "9998")

            Job.objects.get(id=job_id).delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    def test_update_job_status(self):

        with self.mongo_util.me_collection(self.cfg["mongo-jobs-collection"]):

            ori_job_count = Job.objects.count()
            job_id = self.create_job_rec()
            self.assertEqual(ori_job_count, Job.objects.count() - 1)

            runner = self.getRunner()

            # test missing status
            with self.assertRaises(ValueError) as context:
                runner.update_job_status(None, "invalid_status")
            self.assertEqual("Please provide both job_id and status", str(context.exception))

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

            Job.objects.get(id=job_id).delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    def test_get_job_status(self):

        with self.mongo_util.me_collection(self.cfg["mongo-jobs-collection"]):

            ori_job_count = Job.objects.count()
            job_id = self.create_job_rec()
            self.assertEqual(ori_job_count, Job.objects.count() - 1)

            runner = self.getRunner()

            # test missing job_id input
            with self.assertRaises(ValueError) as context:
                runner.get_job_status(None)
            self.assertEqual("Please provide valid job_id", str(context.exception))

            returnVal = runner.get_job_status(job_id)

            self.assertTrue("status" in returnVal)
            self.assertEqual(returnVal["status"], "created")

            Job.objects.get(id=job_id).delete()
            self.assertEqual(ori_job_count, Job.objects.count())
