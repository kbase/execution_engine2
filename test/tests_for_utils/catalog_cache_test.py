# This test only tests code that can be exercised without a network connection to services.
# That code is tested in integration tests.
import pytest

from unittest.mock import create_autospec, MagicMock

from installed_clients.CatalogClient import Catalog
from execution_engine2.utils.catalog_cache import CatalogCache


@pytest.fixture
def catalog():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs = MagicMock(
        return_value={
            "module_name": "module_name",
            "function_name": "function_name",
            "client_groups": "client_groups",
        }
    )
    catalog.get_module_version = MagicMock(return_value={"git_commit_hash": 123})

    return catalog


def test_cc_job_reqs(catalog):
    """Test to see the job requirements cache is being used"""
    catalog_cache = CatalogCache(catalog=catalog)

    # Test Cache is called on second call
    catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )
    assert catalog.list_client_group_configs.call_count == 1
    catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )
    assert catalog.list_client_group_configs.call_count == 1
    catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test2"
    )
    assert catalog.list_client_group_configs.call_count == 2
    catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test2"
    )
    assert catalog.list_client_group_configs.call_count == 2


def test_cc_git_commit_version(catalog):
    """Test to see the git commit cache is being used"""
    catalog_cache = CatalogCache(catalog=catalog)

    # Test Cache is called on second call
    version = catalog_cache.get_git_commit_version(method="method1", service_ver="any")
    assert version == 123
    catalog_cache.get_git_commit_version(method="method1", service_ver="any")
    assert catalog.get_module_version.call_count == 1
    catalog_cache.get_git_commit_version(method="method1", service_ver="any")
    assert catalog.get_module_version.call_count == 1
    catalog_cache.get_git_commit_version(
        method="method1",
    )
    assert catalog.get_module_version.call_count == 2
    catalog_cache.get_git_commit_version(
        method="method1",
    )
    assert catalog.get_module_version.call_count == 2
    catalog_cache.get_git_commit_version(
        method="method2",
    )
    assert catalog.get_module_version.call_count == 3
