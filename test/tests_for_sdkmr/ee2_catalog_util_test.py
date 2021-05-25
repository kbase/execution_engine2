"""
Unit tests for the CatalogCache
"""
import pytest
import os
from test.utils_shared.test_utils import read_config_into_dict
from execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.utils.clients import get_user_client_set, get_client_set


class CatalogCacheHelper:
    def __init__(self):
        self.deploy = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
        config = read_config_into_dict(self.deploy)
        self.cfg = config
        self.user_id = "wsadmin"
        self.ws_id = 9999
        self.token = "token"
        self.ctx = {"token": self.token, "user_id": self.user_id}
        with open(self.deploy) as cf:
            self.runner = SDKMethodRunner(
                get_user_client_set(self.cfg, self.user_id, self.token),
                get_client_set(self.cfg, cf),
            )
        self.catalog_cache = self.runner.get_catalog_cache()


def test_catalog_cache_lookup():

    runner = CatalogCacheHelper().runner
    cc = runner.get_catalog_cache()
    version = cc.get_git_commit_version(method="metabat.run_metabat", service_ver="dev")
    print(version)
