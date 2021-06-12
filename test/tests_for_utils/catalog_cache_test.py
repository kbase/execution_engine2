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

    # Test that a new catalog call is made once more
    with pytest.raises(ValueError) as e:
        catalog_cache = CatalogCache(catalog=catalog)
        catalog_cache.lookup_git_commit_version(method=None, service_ver="dev")
    assert_exception_correct(e.value, ValueError("Must provide a method to lookup"))


def assert_call_count_and_return_val(
    mock, call_count, return_value, expected_return_value
):
    assert mock.call_count == call_count
    assert return_value == expected_return_value


def test_get_catalog(catalog):
    assert catalog == CatalogCache(catalog).get_catalog()


def test_cc_job_reqs(catalog):
    """Test to see the job requirements cache is being used."""
    test_return = {"Test1"}
    catalog.list_client_group_configs.return_value = test_return
    catalog_cache = CatalogCache(catalog=catalog)
    job_reqs_cache = catalog_cache.get_job_resources_cache()

    # Test Cache is called on second call
    rv1 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )

    assert catalog.list_client_group_configs.call_count == 1
    # Test to make sure it still returns values based on the catalog
    assert rv1 == test_return
    assert "test1" in job_reqs_cache and "test1" in job_reqs_cache["test1"]
    catalog.list_client_group_configs.assert_called_with(
        {"module_name": "test1", "function_name": "test1"}
    )

    catalog.list_client_group_configs.return_value = CLIENT_GROUP_CONFIG
    catalog_cache._job_requirements_cache["test1"]["test1"] = "Something else"
    rv2 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )
    # Test to make sure the catalog cache is being used this time, even though the underlying catalog record changed
    assert rv2 != CLIENT_GROUP_CONFIG
    assert rv2 == "Something else"
    catalog.list_client_group_configs.assert_called_with(
        {"module_name": "test1", "function_name": "test1"}
    )

    # Test to see a new catalog call is made
    assert catalog.list_client_group_configs.call_count == 1
    catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test2"
    )
    assert catalog.list_client_group_configs.call_count == 2
    assert "test1" in job_reqs_cache and "test2" in job_reqs_cache["test1"]
    catalog.list_client_group_configs.assert_called_with(
        {"module_name": "test1", "function_name": "test2"}
    )


def test_cc_git_commit_version(catalog):
    """Test to see the git commit cache is being used."""
    catalog_cache = CatalogCache(catalog=catalog)
    catalog_git_return_1 = {"git_commit_hash": "1234"}
    catalog_git_return_2 = {"git_commit_hash": "12345"}
    catalog.get_module_version.return_value = catalog_git_return_1
    method_version_cache = catalog_cache.get_method_version_cache()

    # Test Cache is called on second call
    version = catalog_cache.lookup_git_commit_version(
        method="method1", service_ver="any"
    )

    # Test to make sure return_value is correct
    assert version == catalog_git_return_1["git_commit_hash"]
    catalog.get_module_version.assert_called_with(
        {"module_name": "method1", "version": "any"}
    )

    # Test to make sure same commit is returned regardless of underlying catalog data
    catalog.get_module_version.return_value = catalog_git_return_2
    version2 = catalog_cache.lookup_git_commit_version(
        method="method1", service_ver="any"
    )
    assert version2 == catalog_git_return_1["git_commit_hash"]
    catalog.get_module_version.assert_called_with(
        {"module_name": "method1", "version": "any"}
    )

    catalog_cache.lookup_git_commit_version(method="method1", service_ver="any")
    assert catalog.get_module_version.call_count == 1
    catalog.get_module_version.assert_called_with(
        {"module_name": "method1", "version": "any"}
    )
    catalog_cache.lookup_git_commit_version(
        method="method1",
    )
    assert catalog.get_module_version.call_count == 2
    catalog.get_module_version.assert_called_with(
        {"module_name": "method1", "version": "release"}
    )

    assert method_version_cache["method1"] == {"any": "1234", "release": "12345"}

    # Test None defaults to release case
    catalog_cache.lookup_git_commit_version(method="method3", service_ver=None)
    catalog.get_module_version.assert_called_with(
        {"module_name": "method3", "version": "release"}
    )

    assert None not in catalog_cache.get_method_version_cache()["method3"]
    assert catalog_cache.get_method_version_cache()["method3"]["release"]
    catalog.get_module_version.assert_called_with(
        {"module_name": "method3", "version": "release"}
    )

    # Test module_name = method.split(".")[0] and call count
    call_count = catalog.get_module_version.call_count
    catalog_cache.lookup_git_commit_version(
        method="MEGAHIT.run_megahit", service_ver="dev"
    )
    catalog.get_module_version.assert_called_with(
        {"module_name": "MEGAHIT", "version": "dev"}
    )
    assert catalog.get_module_version.call_count == call_count + 1

    # Test that the catalog is not called, from cache now
    catalog_cache.lookup_git_commit_version(
        method="MEGAHIT.run_megahit", service_ver="dev"
    )
    assert catalog.get_module_version.call_count == call_count + 1

    # Test that a new catalog call is made once more
    catalog_cache.lookup_git_commit_version(
        method="MEGAHIT.run_megahit2", service_ver="dev"
    )
    catalog.get_module_version.assert_called_with(
        {"module_name": "MEGAHIT", "version": "dev"}
    )

    assert method_version_cache["MEGAHIT.run_megahit"] == {"dev": "12345"}
    assert method_version_cache["MEGAHIT.run_megahit2"] == {"dev": "12345"}
