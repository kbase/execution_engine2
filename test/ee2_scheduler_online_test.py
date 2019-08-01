# -*- coding: utf-8 -*-

import logging
import unittest

logging.basicConfig(level=logging.INFO)

from execution_engine2.utils.Condor import Condor
from lib.installed_clients.execution_engine2Client import execution_engine2
import os, sys

from dotenv import load_dotenv

load_dotenv("env/test.env", verbose=True)


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        This test is used for sending commands to a live environment, such as CI.
        TravisCI doesn't need to run this test.
        :return:
        """
        if "KB_AUTH_TOKEN" not in os.environ or "ENDPOINT" not in os.environ:
            logging.error(
                "Make sure you copy the env/test.env.example file to test/env/test.env and populate it"
            )
            sys.exit(1)

        cls.ee2 = execution_engine2(
            url=os.environ["ENDPOINT"], token=os.environ["KB_AUTH_TOKEN"]
        )

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "wsName"):
            cls.wsClient.delete_workspace({"workspace": cls.wsName})
            print("Test workspace was deleted")

    def test_status(self):
        """
        Test to see if the ee2 service is up
        :return:
        """
        self.assertIn("servertime", self.ee2.status())

    def test_run_job(self):
        """
        Test a simple job baed on runjob params from the spec file

        typedef structure {
            string method;
            list<UnspecifiedObject> params;
            string service_ver;
            RpcContext rpc_context;
            string remote_url;
            list<wsref> source_ws_objects;
            string app_id;
            mapping<string, string> meta;
            int wsid;
            string parent_job_id;
        } RunJobParams;

        :return:
        """
        params = {"base_number": "105"}
        runjob_params = {
            "method": "simpleapp.simple_add",
            "params": params,
            "service_ver": "dev",
        }

        print(self.ee2.run_job(runjob_params))
