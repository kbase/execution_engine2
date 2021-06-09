# This test only tests code that can be exercised without a network connection to services.
# That code is tested in integration tests.
from unittest.mock import create_autospec

import pytest

from execution_engine2.utils.catalog_cache import CatalogCache
from installed_clients.CatalogClient import Catalog
from utils_shared.test_utils import (
    assert_exception_correct,
    CLIENT_GROUP_CONFIG,
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
    test_return = {"Test1"}
    catalog.list_client_group_configs.return_value = test_return
    catalog_cache = CatalogCache(catalog=catalog)

    # Test Cache is called on second call
    rv1 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )
    assert catalog.list_client_group_configs.call_count == 1
    # Test to make sure it still returns values based on the catalog
    assert rv1 == test_return

    catalog.list_client_group_configs.return_value = CLIENT_GROUP_CONFIG
    catalog_cache._method_version_cache["test1"]["test1"] = "Something else"
    rv2 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )
    # Test to make sure the catalog cache is being used this time, even though the underlying catalog record changed
    assert rv2 != CLIENT_GROUP_CONFIG
    assert rv2 == test_return

    # Test to see a new catalog call is made
    assert catalog.list_client_group_configs.call_count == 1
    catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test2"
    )
    assert catalog.list_client_group_configs.call_count == 2


def test_cc_git_commit_version(catalog):
    """Test to see the git commit cache is being used."""
    catalog_cache = CatalogCache(catalog=catalog)
    catalog_git_return_1 = {"git_commit_hash": "1234"}
    catalog_git_return_2 = {"git_commit_hash": "12345"}
    catalog.get_module_version.return_value = catalog_git_return_1

    # Test Cache is called on second call
    version = catalog_cache.get_git_commit_version(method="method1", service_ver="any")

    # Test to make sure return_value is correct
    assert version == catalog_git_return_1["git_commit_hash"]

    # Test to make sure same commit is returned regardless of underlying catalog data
    catalog.get_module_version.return_value = catalog_git_return_2
    version2 = catalog_cache.get_git_commit_version(method="method1", service_ver="any")
    assert version2 == catalog_git_return_1["git_commit_hash"]

    catalog_cache.get_git_commit_version(method="method1", service_ver="any")
    assert catalog.get_module_version.call_count == 1
    catalog_cache.get_git_commit_version(
        method="method1",
    )
    assert catalog.get_module_version.call_count == 2

    # Test None defaults to release case
    catalog_cache.get_git_commit_version(method="method3", service_ver=None)
    assert None not in catalog_cache.get_method_version_cache()["method3"]
    assert catalog_cache.get_method_version_cache()["method3"]["release"]

    # Test module_name = method.split(".")[0] and call count
    call_count = catalog.get_module_version.call_count
    catalog_cache.get_git_commit_version(
        method="MEGAHIT.run_megahit", service_ver="dev"
    )
    catalog.get_module_version.assert_called_with(
        {"module_name": "MEGAHIT", "version": "dev"}
    )
    assert catalog.get_module_version.call_count == call_count + 1

    # Test that the catalog is not called, from cache now
    catalog_cache.get_git_commit_version(
        method="MEGAHIT.run_megahit", service_ver="dev"
    )
    assert catalog.get_module_version.call_count == call_count + 1

    # Test that a new catalog call is made once more
    catalog_cache.get_git_commit_version(
        method="MEGAHIT.run_megahit2", service_ver="dev"
    )
    catalog.get_module_version.assert_called_with(
        {"module_name": "MEGAHIT", "version": "dev"}
    )
    assert catalog.get_module_version.call_count == call_count + 2
