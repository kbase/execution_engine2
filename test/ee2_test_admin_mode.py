# -*- coding: utf-8 -*-
from execution_engine2.SDKMethodRunner import SDKMethodRunner
import unittest
from test.utils.test_utils import bootstrap

bootstrap()
import os
from configparser import ConfigParser
import copy
from installed_clients.CatalogClient import Catalog
from mock import patch
from execution_engine2.utils.Condor import Condor, submission_info
import bson


class EE2TestAdminMode(unittest.TestCase):
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

        cls.user_id = "wsadmin"
        cls.ws_id = 9999
        cls.token = "token"

        cls.method_runner = SDKMethodRunner(
            cls.cfg, user_id=cls.user_id, token=cls.token
        )

    def setUp(self) -> None:
        """
        Patch out Catalog and Condor
        :return:
        """
        self.catalog_patch = patch(
            "lib.installed_clients.CatalogClient.Catalog.get_module_version"
        )
        self.catalog = self.catalog_patch.start()
        self.catalog.return_value = {"git_commit_hash": "moduleversiongoeshere"}

        si = submission_info(clusterid="test", submit="job", error=None)
        self.condor_patch = patch.object(Condor, "run_job", return_value=si)
        self.condor = self.condor_patch.start()
        # return self.condor_value = si

    def tearDown(self) -> None:
        self.catalog_patch.stop()

    def getRunner(self) -> SDKMethodRunner:
        # Initialize these clients from None
        runner = copy.deepcopy(self.__class__.method_runner)  # type : SDKMethodRunner
        runner.get_jobs_status()
        runner.get_runjob()
        runner.get_job_logs()
        return runner

    def get_sample_job_params(self, method=None):

        if not method:
            method = "default_method"

        job_params = {
            "wsid": self.ws_id,
            "method": method,
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
            "parent_job_id": "9998",
            "meta": {"tag": "dev", "token_id": "12345"},
        }

        return job_params

    # TODO How do you test ADMIN_MODE without increasing too much coverage

    def test_runjob(self):

        runner = self.getRunner()
        method_1 = "module_name.function_name"
        job_params_1 = self.get_sample_job_params(method=method_1)
        with self.assertRaisesRegexp(
            expected_exception=RuntimeError,
            expected_regex=r"ServerError\('Token validation failed: Login failed! Server responded with code 401 Unauthorized'\)",
        ):
            runner.run_job(params=job_params_1, as_admin=False)

        job_id = runner.run_job(params=job_params_1, as_admin=True)
        self.assertTrue(bson.objectid.ObjectId.is_valid(job_id))
