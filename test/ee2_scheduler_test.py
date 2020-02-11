# -*- coding: utf-8 -*-
import logging
import unittest

logging.basicConfig(level=logging.INFO)

from execution_engine2.utils.CatalogUtils import CatalogUtils
from execution_engine2.utils.Condor import Condor
from test.test_utils import bootstrap

import os

bootstrap()


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.deploy = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
        cls.condor = Condor("deploy.cfg")
        cls.job_id = "1234"
        cls.user = "kbase"
        cls.catalog_utils = CatalogUtils(url="https://ci.kbase.us/services/Catalog")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "wsName"):
            cls.wsClient.delete_workspace({"workspace": cls.wsName})
            print("Test workspace was deleted")

    def _create_sample_params(self, cgroups):
        params = dict()
        params["job_id"] = self.job_id
        params["user_id"] = "kbase"
        params["token"] = "test_token"
        rr = CatalogUtils.normalize_catalog_cgroups(cgroups)

        print(rr)
        params["cg_resources_requirements"] = rr

        return params

    def test_empty_params(self):
        c = self.condor
        params = {"job_id": "test_job_id", "user_id": "test", "token": "test_token"}
        with self.assertRaisesRegex(
            Exception, "cg_resources_requirements not found in params"
        ):
            c.create_submit(params)

    def test_create_submit_file(self):
        # Test with empty clientgroup
        logging.info("Testing with njs clientgroup")
        c = self.condor
        params = self._create_sample_params(cgroups=["njs"])

        default_sub = c.create_submit(params)

        sub = default_sub
        self.assertEqual(sub["executable"], c.initial_dir + "/" + c.executable)
        self.assertEqual(sub["arguments"], f"{params['job_id']} {c.ee_endpoint}")
        self.assertEqual(sub["universe"], "vanilla")
        self.assertEqual(sub["+AccountingGroup"], params["user_id"])
        self.assertEqual(sub["Concurrency_Limits"], params["user_id"])
        self.assertEqual(sub["+Owner"], '"condor_pool"')
        self.assertEqual(sub["ShouldTransferFiles"], "YES")
        self.assertEqual(sub["When_To_Transfer_Output"], "ON_EXIT")
        # TODO test config here or otherplace
        # self.assertEqual(sub["+CLIENTGROUP"], "njs")
        self.assertEqual(sub[Condor.REQUEST_CPUS], c.config["njs"][Condor.REQUEST_CPUS])
        self.assertEqual(
            sub[Condor.REQUEST_MEMORY], c.config["njs"][Condor.REQUEST_MEMORY]
        )
        self.assertEqual(sub[Condor.REQUEST_DISK], c.config["njs"][Condor.REQUEST_DISK])

        # TODO Test this variable somehow
        # environment = sub["environment"].split(" ")

        # Test with filled out clientgroup
        logging.info("Testing with complex-empty clientgroup")

        params = self._create_sample_params(
            cgroups=["njs,request_cpus=8,request_memory=10GB,request_apples=5"]
        )

        njs_sub = c.create_submit(params)
        sub = njs_sub

        self.assertIn("njs", sub["requirements"])

        self.assertIn('regexp("njs",CLIENTGROUP)', sub["requirements"])

        self.assertIn('request_apples == "5"', sub["requirements"])

        self.assertEqual(sub[Condor.REQUEST_CPUS], "8")
        self.assertEqual(sub[Condor.REQUEST_MEMORY], "10GB")
        self.assertEqual(sub[Condor.REQUEST_DISK], c.config["njs"][Condor.REQUEST_DISK])

        logging.info("Testing with regex disabled in old format (no effect)")

        # with self.assertRaisesRegex(
        #     ValueError, "Illegal argument! Old format does not support this option"
        # ):
        #     params = self._create_sample_params(
        #         cgroups=["njs,request_cpus=8,request_memory=10GB,request_apples=5,client_group_regex=False"]
        #     )
        #     c.create_submit(params)  # pragma: no cover

        # Test with json version of clientgroup

        logging.info("Testing with empty clientgroup defaulting to njs")

        params = self._create_sample_params(cgroups="")

        empty_sub = c.create_submit(params)
        sub = empty_sub

        self.assertEqual(sub[Condor.REQUEST_CPUS], c.config["njs"][Condor.REQUEST_CPUS])
        self.assertEqual(
            sub[Condor.REQUEST_MEMORY], c.config["njs"][Condor.REQUEST_MEMORY]
        )
        self.assertEqual(sub[Condor.REQUEST_DISK], c.config["njs"][Condor.REQUEST_DISK])

        # logging.info("Testing with empty dict (raises typeerror)")
        #
        # with self.assertRaises(TypeError):
        #     params = self._create_sample_params(cgroups={})
        #     print(params)
        #     empty_json_sub = c.create_submit(params)

        logging.info("Testing with empty dict as a string ")

        params = self._create_sample_params(cgroups=["{}"])

        empty_json_sub = c.create_submit(params)

        params = self._create_sample_params(cgroups=['{"client_group" : "njs"}'])

        json_sub = c.create_submit(params)

        params = self._create_sample_params(
            cgroups=['{"client_group" : "njs", "client_group_regex" : "false"}']
        )

        json_sub_with_regex_disabled_njs = c.create_submit(params)

        # json_sub_with_regex_disabled

        logging.info("Testing with real valid json ")
        for sub in [empty_json_sub, json_sub, json_sub_with_regex_disabled_njs]:
            self.assertEqual(
                sub[Condor.REQUEST_CPUS], c.config["njs"][Condor.REQUEST_CPUS]
            )
            self.assertEqual(
                sub[Condor.REQUEST_MEMORY], c.config["njs"][Condor.REQUEST_MEMORY]
            )
            self.assertEqual(
                sub[Condor.REQUEST_DISK], c.config["njs"][Condor.REQUEST_DISK]
            )

        with self.assertRaises(ValueError):
            logging.info("Testing with real json invalid cgroup {bigmemzlong} ")
            params = self._create_sample_params(
                cgroups='{"client_group" : "bigmemzlong", "client_group_regex" : "FaLsE"}'
            )

            # json_sub_with_regex_disabled
            c.create_submit(params)

        logging.info("Testing with real json, regex disabled, bigmem")

        params = self._create_sample_params(
            cgroups=['{"client_group" : "bigmem", "client_group_regex" : "FaLsE"}']
        )

        json_sub_with_regex_disabled_bigmem = c.create_submit(params)
        self.assertIn(
            '(CLIENTGROUP == "bigmem',
            json_sub_with_regex_disabled_bigmem["requirements"],
        )
