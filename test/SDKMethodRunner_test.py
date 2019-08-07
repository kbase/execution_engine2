# -*- coding: utf-8 -*-
import os
import unittest
from configparser import ConfigParser
from mongoengine import ValidationError

from bson.objectid import ObjectId

from execution_engine2.utils.MongoUtil import MongoUtil
from execution_engine2.utils.SDKMethodRunner import SDKMethodRunner
from execution_engine2.models.models import Job, JobInput, Meta
from test.mongo_test_helper import MongoTestHelper
from test.test_utils import bootstrap

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

        cls.cfg["start-local-mongo"] = "1"

        cls.method_runner = SDKMethodRunner(cls.cfg)
        cls.mongo_util = MongoUtil(cls.cfg)
        cls.mongo_helper = MongoTestHelper(cls.cfg)

        cls.test_collection = cls.mongo_helper.create_test_db(
            db=cls.cfg["mongo-database"], col=cls.cfg["mongo-jobs-collection"]
        )

        cls.user_id = "fake_test_user"
        cls.ws_id = 9999

    def getRunner(self):
        return self.__class__.method_runner

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
            "parent_job_id": "9998"
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

    #     [info1, info2] = self.foft.create_fake_reads(
    #         {"ws_name": self.wsName, "obj_names": ["reads1", "reads2"]}
    #     )
    #     read1ref = str(info1[6]) + "/" + str(info1[0]) + "/" + str(info1[4])
    #     read2ref = str(info2[6]) + "/" + str(info2[0]) + "/" + str(info2[4])

    #     runner._check_ws_objects([read1ref, read2ref])

    #     fake_read1ref = str(info1[6]) + "/" + str(info1[0]) + "/" + str(info1[4] + 100)

    #     with self.assertRaises(ValueError) as context:
    #         runner._check_ws_objects([read1ref, read2ref, fake_read1ref])

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

        runner = self.getRunner()

        self.assertEqual(self.test_collection.count(), 0)

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

        self.assertEqual(self.test_collection.count(), 1)

        result = list(self.test_collection.find({"_id": ObjectId(job_id)}))[0]

        expected_keys = ["_id", "user", "authstrat", "wsid", "status", "updated", "job_input"]

        self.assertCountEqual(result.keys(), expected_keys)
        self.assertEqual(result["user"], self.user_id)
        self.assertEqual(result["authstrat"], "kbaseworkspace")
        self.assertEqual(result["wsid"], self.ws_id)

        job_input = result["job_input"]
        expected_ji_keys = ["wsid", "method", "params", "service_ver", "app_id",
                            "narrative_cell_info", "source_ws_objects", "parent_job_id"]
        self.assertCountEqual(job_input.keys(), expected_ji_keys)
        self.assertEqual(job_input["wsid"], self.ws_id)
        self.assertEqual(job_input["method"], "MEGAHIT.run_megahit")
        self.assertEqual(job_input["app_id"], "MEGAHIT/run_megahit")
        self.assertEqual(job_input["service_ver"], "2.2.1")
        self.assertCountEqual(job_input["source_ws_objects"], ["a/b/c", "e/d"])
        self.assertEqual(job_input["parent_job_id"], "9998")

        self.assertFalse(result.get("job_output"))

        self.test_collection.delete_one({"_id": ObjectId(job_id)})
        self.assertEqual(self.test_collection.count(), 0)

    def test_get_job_params(self):

        self.assertEqual(self.test_collection.count(), 0)
        job_id = self.create_job_rec()
        self.assertEqual(self.test_collection.count(), 1)

        runner = self.getRunner()
        params,  config = runner.get_job_params(job_id)

        expected_params_keys = ["wsid", "method", "params", "service_ver", "app_id",
                                "source_ws_objects", "parent_job_id"]
        self.assertCountEqual(params.keys(), expected_params_keys)
        self.assertEqual(params["wsid"], self.ws_id)
        self.assertEqual(params["method"], "MEGAHIT.run_megahit")
        self.assertEqual(params["app_id"], "MEGAHIT/run_megahit")
        self.assertEqual(params["service_ver"], "2.2.1")
        self.assertCountEqual(params["source_ws_objects"], ["a/b/c", "e/d"])
        self.assertEqual(params["parent_job_id"], "9998")

        self.assertFalse(config)

        self.test_collection.delete_one({"_id": ObjectId(job_id)})
        self.assertEqual(self.test_collection.count(), 0)

    def test_update_job_status(self):

        self.assertEqual(self.test_collection.count(), 0)
        job_id = self.create_job_rec()
        self.assertEqual(self.test_collection.count(), 1)

        runner = self.getRunner()

        # test invalid status
        with self.assertRaises(ValidationError) as context:
            runner.update_job_status(job_id, "invalid_status")

        self.assertIn("is not a valid status", str(context.exception))

        job_id = runner.update_job_status(job_id, "estimating")
        result = list(self.test_collection.find({"_id": ObjectId(job_id)}))[0]

        self.assertEqual(result["status"], "estimating")

        self.test_collection.delete_one({"_id": ObjectId(job_id)})
        self.assertEqual(self.test_collection.count(), 0)
