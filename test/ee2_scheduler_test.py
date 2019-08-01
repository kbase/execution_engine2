# -*- coding: utf-8 -*-
import logging
import unittest

logging.basicConfig(level=logging.INFO)

from execution_engine2.utils.Condor import Condor


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.deploy = "deploy.cfg"
        cls.condor = Condor(cls.deploy)
        cls.job_id = "1234"
        cls.user = "kbase"

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "wsName"):
            cls.wsClient.delete_workspace({"workspace": cls.wsName})
            print("Test workspace was deleted")

    def _create_sample_params(self):
        params = dict()
        params["job_id"] = self.job_id
        params["user_id"] = "kbase"
        params["token"] = "test_token"
        params["cg_resources_requirements"] = "njs"
        return params

    def test_empty_params(self):
        c = Condor("deploy.cfg")
        params = {"job_id": "test_job_id", "user_id": "test", "token": "test_token"}
        with self.assertRaisesRegex(
            Exception, "cg_resources_requirements not found in params"
        ) as error:
            c.create_submit(params)

    def test_create_submit_file(self):
        # Test with empty clientgroup
        logging.info("Testing with njs clientgroup")
        c = Condor("deploy.cfg")
        params = self._create_sample_params()

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
        environment = sub["environment"].split(" ")

        # Test with filled out clientgroup
        logging.info("Testing with complex-empty clientgroup")
        params[
            "cg_resources_requirements"
        ] = "njs,request_cpus=8,request_memory=10GB,request_apples=5"

        njs_sub = c.create_submit(params)
        sub = njs_sub

        self.assertIn("njs", sub["requirements"])

        self.assertIn('regexp("njs",CLIENTGROUP)', sub["requirements"])

        self.assertIn('request_apples == "5"', sub["requirements"])

        self.assertEqual(sub[Condor.REQUEST_CPUS], "8")
        self.assertEqual(sub[Condor.REQUEST_MEMORY], "10GB")
        self.assertEqual(sub[Condor.REQUEST_DISK], c.config["njs"][Condor.REQUEST_DISK])

        params[
            "cg_resources_requirements"
        ] = "njs,request_cpus=8,request_memory=10GB,request_apples=5,client_group_regex=False"

        logging.info("Testing with regex disabled in old format (no effect)")

        with self.assertRaisesRegex(
            ValueError, "Illegal argument! Old format does not support this option"
        ) as error:
            regex_sub = c.create_submit(params)  # pragma: no cover
            sub = njs_sub  # pragma: no cover

        # Test with json version of clientgroup

        logging.info("Testing with empty clientgroup defaulting to njs")

        params["cg_resources_requirements"] = ""

        empty_sub = c.create_submit(params)
        sub = empty_sub

        self.assertEqual(sub[Condor.REQUEST_CPUS], c.config["njs"][Condor.REQUEST_CPUS])
        self.assertEqual(
            sub[Condor.REQUEST_MEMORY], c.config["njs"][Condor.REQUEST_MEMORY]
        )
        self.assertEqual(sub[Condor.REQUEST_DISK], c.config["njs"][Condor.REQUEST_DISK])

        logging.info("Testing with empty dict (raises typeerror)")

        params["cg_resources_requirements"] = {}

        with self.assertRaises(TypeError) as e:
            empty_json_sub = c.create_submit(params)

        logging.info("Testing with empty dict as a string ")

        params["cg_resources_requirements"] = "{}"
        empty_json_sub = c.create_submit(params)

        params["cg_resources_requirements"] = '{"client_group" : "njs"}'
        json_sub = c.create_submit(params)

        params[
            "cg_resources_requirements"
        ] = '{"client_group" : "njs", "client_group_regex" : "FaLsE"}'
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

        with self.assertRaises(ValueError) as e:
            logging.info("Testing with real json invalid cgroup {bigmemzlong} ")

            params[
                "cg_resources_requirements"
            ] = '{"client_group" : "bigmemzlong", "client_group_regex" : "FaLsE"}'
            json_sub_with_regex_disabled = c.create_submit(params)

        logging.info("Testing with real json, regex disabled, bigmem")
        params[
            "cg_resources_requirements"
        ] = '{"client_group" : "bigmem", "client_group_regex" : "FaLsE"}'
        json_sub_with_regex_disabled_bigmem = c.create_submit(params)
        self.assertIn(
            '(CLIENTGROUP == "bigmem',
            json_sub_with_regex_disabled_bigmem["requirements"],
        )
