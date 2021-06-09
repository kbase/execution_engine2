# This test only tests code that can be exercised without a network connection to services.
# That code is tested in integration tests.
from unittest.mock import create_autospec, MagicMock

import pytest

from installed_clients.CatalogClient import Catalog
from execution_engine2.utils.catalog_cache import CatalogCache
from utils_shared.test_utils import (
    assert_exception_correct,
    CLIENT_GROUP_CONFIG,
    MODULE_VERSION,
)


@pytest.fixture
def catalog():
    return create_autospec(Catalog, spec_set=True, instance=True)


@pytest.fixture
def catalog_cache():
    return create_autospec(CatalogCache, spec_set=True, instance=True)


def test_fail_cc():
    with pytest.raises(ValueError) as e:
        CatalogCache(None)
    assert_exception_correct(
        e.value, ValueError("Please provide instance of catalog client")
    )


def test_cc_getters(catalog):
    """Test getters"""
    catalog_cache = CatalogCache(catalog)
    assert catalog_cache.get_catalog() == catalog
    assert (
        catalog_cache.get_method_version_cache() == catalog_cache._method_version_cache
    )
    assert (
        catalog_cache.get_job_resources_cache() == catalog_cache._job_requirements_cache
    )


def assert_call_count_and_return_val(
    mock, call_count, return_value, expected_return_value
):
    assert mock.call_count == call_count
    assert return_value == expected_return_value


def test_cc_job_reqs(catalog):
    """Test to see the job requirements cache is being used."""
    catalog.list_client_group_configs.return_value = CLIENT_GROUP_CONFIG
    catalog_cache = CatalogCache(catalog=catalog)

    # Test Cache is called on second call
    rv1 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )
    assert catalog.list_client_group_configs.call_count == 1
    assert rv1 == CLIENT_GROUP_CONFIG
    rv2 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )
    assert rv2 == CLIENT_GROUP_CONFIG
    assert catalog.list_client_group_configs.call_count == 1
    rv3 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test2"
    )
    assert rv3 == CLIENT_GROUP_CONFIG
    assert catalog.list_client_group_configs.call_count == 2
    rv4 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test2"
    )
    assert rv4 == CLIENT_GROUP_CONFIG
    assert catalog.list_client_group_configs.call_count == 2


def test_cc_git_commit_version(catalog):
    """Test to see the git commit cache is being used."""
    catalog_cache = CatalogCache(catalog=catalog)

    catalog.get_module_version.return_value = MODULE_VERSION

    # Test Cache is called on second call
    version = catalog_cache.get_git_commit_version(method="method1", service_ver="any")
    assert version == MODULE_VERSION["git_commit_hash"]
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

    # Test None defaults to release case
    catalog_cache.get_git_commit_version(method="method5", service_ver=None)
    assert None not in catalog_cache.get_method_version_cache()["method5"]
    assert catalog_cache.get_method_version_cache()["method5"]["release"]
