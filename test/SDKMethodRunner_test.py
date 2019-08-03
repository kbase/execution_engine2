# -*- coding: utf-8 -*-
import os
import unittest
from configparser import ConfigParser

from bson.objectid import ObjectId

from execution_engine2.utils.MongoUtil import MongoUtil
from execution_engine2.utils.SDKMethodRunner import SDKMethodRunner
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
        }

        job_id = runner._init_job_rec(self.user_id, job_params)

        self.assertEqual(self.test_collection.count(), 1)

        result = list(self.test_collection.find({"_id": ObjectId(job_id)}))[0]

        expected_keys = ['_id', 'user', 'authstrat', 'wsid', 'status', 'updated', 'job_input']

        self.assertCountEqual(result.keys(), expected_keys)
        self.assertEqual(result["user"], self.user_id)
        self.assertEqual(result["authstrat"], "kbaseworkspace")
        self.assertEqual(result["wsid"], self.ws_id)

        job_input = result["job_input"]
        expected_ji_keys = ["wsid", "method", "params", "service_ver", "app_id", "narrative_cell_info"]
        self.assertCountEqual(job_input.keys(), expected_ji_keys)
        self.assertEqual(job_input["wsid"], self.ws_id)
        self.assertEqual(job_input["method"], "MEGAHIT.run_megahit")
        self.assertEqual(job_input["app_id"], "MEGAHIT/run_megahit")
        self.assertEqual(job_input["service_ver"], "2.2.1")

        self.assertFalse(result.get("job_output"))

        self.test_collection.delete_one({"_id": ObjectId(job_id)})
        self.assertEqual(self.test_collection.count(), 0)
