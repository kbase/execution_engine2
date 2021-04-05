# -*- coding: utf-8 -*-

"""
Tests for the Condor scheduler.
"""

import logging
import os
import unittest

from lib.execution_engine2.utils.Condor import Condor
from execution_engine2.sdk.job_submission_parameters import (
    JobSubmissionParameters,
    JobRequirements,
)
from execution_engine2.utils.job_requirements_resolver import (
    REQUEST_CPUS,
    REQUEST_DISK,
    REQUEST_MEMORY,
)
from execution_engine2.utils.application_info import AppInfo
from execution_engine2.utils.user_info import UserCreds
from test.utils_shared.test_utils import bootstrap, get_ee2_test_config

logging.basicConfig(level=logging.INFO)

bootstrap()


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.condor = Condor(get_ee2_test_config())
        cls.job_id = "1234"
        cls.user = "kbase"

    def _create_sample_params(self, request_x, scheduler_requirements=None):
        sr = scheduler_requirements if scheduler_requirements else {}
        return JobSubmissionParameters(
            self.job_id,
            AppInfo("fake.fake", "fake/app"),
            JobRequirements(
                request_x.get("request_cpus", 101),
                request_x.get("request_memory", 102),
                request_x.get("request_disk", 103),
                request_x.get("client_group", "defaultcg"),
                request_x.get("client_group_regex", True),
                bill_to_user=request_x.get("bill_to_user"),
                ignore_concurrency_limits=request_x.get("ignore_concurrency_limits", False),
                scheduler_requirements=sr,
            ),
            UserCreds(self.user, "test_token"),
        )

    def test_create_submit_file(self):
        # Test with empty clientgroup
        logging.info("Testing with njs clientgroup")
        c = self.condor
        params = self._create_sample_params({"client_group": "njs"})

        default_sub = c._create_submit(params)

        sub = default_sub
        self.assertEqual(sub["executable"], c.initial_dir + "/" + c.executable)
        self.assertEqual(sub["arguments"], f"{self.job_id} {c.ee_endpoint}")
        self.assertEqual(sub["universe"], "vanilla")
        self.assertEqual(sub["+AccountingGroup"], f'"{self.user}"')
        self.assertEqual(sub["Concurrency_Limits"], self.user)
        self.assertEqual(sub["+Owner"], '"condor_pool"')
        self.assertEqual(sub["ShouldTransferFiles"], "YES")
        self.assertEqual(sub["When_To_Transfer_Output"], "ON_EXIT_OR_EVICT")

        self.assertEqual(sub[REQUEST_CPUS], "101")
        self.assertEqual(sub[REQUEST_MEMORY], "102MB")
        self.assertEqual(sub[REQUEST_DISK], "103GB")

        # TODO Test this variable somehow
        # environment = sub["environment"].split(" ")

        # Test with filled out clientgroup
        logging.info("Testing with complex-empty clientgroup")

        params = self._create_sample_params(
            {"client_group": "njs", "request_cpus": 8, "request_memory": 10},
            {"request_apples": "5"}
        )

        njs_sub = c._create_submit(params)
        sub = njs_sub

        self.assertIn("njs", sub["requirements"])

        self.assertIn('regexp("njs",CLIENTGROUP)', sub["requirements"])

        self.assertIn('request_apples == "5"', sub["requirements"])

        self.assertEqual(sub[REQUEST_CPUS], "8")
        self.assertEqual(sub[REQUEST_MEMORY], "10MB")
        self.assertEqual(sub[REQUEST_DISK], "103GB")

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

        params = self._create_sample_params({})

        empty_sub = c._create_submit(params)
        sub = empty_sub

        self.assertEqual(sub[REQUEST_CPUS], "101")
        self.assertEqual(sub[REQUEST_MEMORY], "102MB")
        self.assertEqual(sub[REQUEST_DISK], "103GB")

        # logging.info("Testing with empty dict (raises typeerror)")
        #
        # with self.assertRaises(TypeError):
        #     params = self._create_sample_params(cgroups={})
        #     print(params)
        #     empty_json_sub = c.create_submit(params)

        logging.info("Testing with regex disabled, bigmem")

        params = self._create_sample_params(
            {"client_group": "bigmem", "client_group_regex": False})

        sub_with_regex_disabled_bigmem = c._create_submit(params)
        self.assertIn(
            '(CLIENTGROUP == "bigmem',
            sub_with_regex_disabled_bigmem["requirements"],
        )

    def test_create_submit_file_without_concurrency_limits_and_bill_to_user(self):
        logging.info("Testing with concierge clientgroup")
        c = self.condor
        params = self._create_sample_params(
            {"client_group": "njs",
             "request_cpus": 100,
             "request_memory": 200,
             "request_disk": 1000,
             "ignore_concurrency_limits": True}
        )
        sub = c._create_submit(params=params)
        # Concurrency limits removed
        self.assertNotIn("Concurrency_Limits", sub)
        self.assertEqual(sub["+AccountingGroup"], f'"{self.user}"')
        self.assertEqual(sub[REQUEST_CPUS], "100")
        self.assertEqual(sub[REQUEST_MEMORY], "200MB")
        self.assertEqual(sub[REQUEST_DISK], "1000GB")
        self.assertEqual(sub["+KB_CLIENTGROUP"], '"njs"')

        params = self._create_sample_params(
            {"client_group": "LeConcierge",
             "bill_to_user": "LeCat",
             "ignore_concurrency_limits": True,
             }
        )
        sub2 = c._create_submit(params=params)
        self.assertEqual(sub2["+KB_CLIENTGROUP"], '"LeConcierge"')
        self.assertEqual(sub2["+AccountingGroup"], '"LeCat"')
        self.assertNotIn("Concurrency_Limits", sub2)

        # submission_info = c.run_submit(sub2)
        #
        # self.assertIsNotNone(submission_info.clusterid)
        # self.assertIsNotNone(submission_info.submit)
        # self.assertIsNone(submission_info.error)

    #
    # def test_extract(self):
    #     logging.info("Testing with concierge clientgroup")
    #     c = self.condor
    #     params = self._create_sample_params(cgroups=["njs"])
    #     cp = self._get_concierge_params()
    #     sub = c.create_submit(params=params, concierge_params=cp)
    #     submission_info = c.run_submit(sub)
    #     print(submission_info)
    #
    #
    # def test_get_usage(self):
    #     job_id = '732'
    #     print(self.condor.get_job_resource_info(cluster_id=job_id))
