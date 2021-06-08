# This test only tests code that can be exercised without a network connection to services.
# That code is tested in integration tests.
import pytest

from unittest.mock import create_autospec, MagicMock

from installed_clients.CatalogClient import Catalog
from execution_engine2.utils.catalog_cache import CatalogCache
from utils_shared.test_utils import assert_exception_correct

CLIENT_GROUP_CONFIG = {
    "module_name": "module_name",
    "function_name": "function_name",
    "client_groups": "client_groups_go_here",
}
MODULE_VERSION = {"git_commit_hash": 123}


@pytest.fixture
def catalog():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = CLIENT_GROUP_CONFIG
    catalog.get_module_version.return_value = MODULE_VERSION
    return catalog


def test_fail_cc():
    with pytest.raises(ValueError) as e:
        CatalogCache(None)
    assert_exception_correct(
        e.value, ValueError("Please provide instance of catalog client")
    )


def test_cc_getters(catalog):
    """Test getters"""
    catalog_cache = CatalogCache(catalog=catalog)
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

    # Test to see that the list_client_group configs is called only when a new entry is requested
    # by checking the call count

    catalog_cache = CatalogCache(catalog=catalog)
    lcgc_mock = catalog.list_client_group_configs

    rv = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )
    assert_call_count_and_return_val(
        mock=lcgc_mock,
        call_count=1,
        return_value=rv,
        expected_return_value=CLIENT_GROUP_CONFIG,
    )

    rv2 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test1"
    )
    assert_call_count_and_return_val(
        mock=lcgc_mock,
        call_count=1,
        return_value=rv2,
        expected_return_value=CLIENT_GROUP_CONFIG,
    )

    rv3 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test2"
    )
    assert_call_count_and_return_val(
        mock=lcgc_mock,
        call_count=2,
        return_value=rv3,
        expected_return_value=CLIENT_GROUP_CONFIG,
    )

    rv4 = catalog_cache.lookup_job_resource_requirements(
        module_name="test1", function_name="test2"
    )
    assert_call_count_and_return_val(
        mock=lcgc_mock,
        call_count=2,
        return_value=rv4,
        expected_return_value=CLIENT_GROUP_CONFIG,
    )


def test_cc_git_commit_version(catalog):
    """Test to see the git commit cache is being used."""
    catalog_cache = CatalogCache(catalog=catalog)
    gmv_mock = catalog.get_module_version
    mock_version = MODULE_VERSION["git_commit_hash"]

    # Test Cache is called on second call
    version = catalog_cache.get_git_commit_version(method="method1", service_ver="any")
    assert_call_count_and_return_val(
        mock=gmv_mock,
        call_count=1,
        return_value=version,
        expected_return_value=mock_version,
    )

    version2 = catalog_cache.get_git_commit_version(method="method1", service_ver="any")
    assert_call_count_and_return_val(
        mock=gmv_mock,
        call_count=1,
        return_value=version2,
        expected_return_value=mock_version,
    )
    version3 = catalog_cache.get_git_commit_version(method="method1", service_ver="any")
    assert_call_count_and_return_val(
        mock=gmv_mock,
        call_count=1,
        return_value=version3,
        expected_return_value=mock_version,
    )

    version4 = catalog_cache.get_git_commit_version(
        method="method1",
    )
    assert_call_count_and_return_val(
        mock=gmv_mock,
        call_count=2,
        return_value=version4,
        expected_return_value=mock_version,
    )

    version5 = catalog_cache.get_git_commit_version(
        method="method1",
    )
    assert_call_count_and_return_val(
        mock=gmv_mock,
        call_count=2,
        return_value=version5,
        expected_return_value=mock_version,
    )

    version6 = catalog_cache.get_git_commit_version(
        method="method2",
    )
    assert_call_count_and_return_val(
        mock=gmv_mock,
        call_count=3,
        return_value=version6,
        expected_return_value=mock_version,
    )

    version7 = catalog_cache.get_git_commit_version(method="method5", service_ver=None)
    assert_call_count_and_return_val(
        mock=gmv_mock,
        call_count=4,
        return_value=version7,
        expected_return_value=mock_version,
    )
    # Test None defaults to release case
    assert None not in catalog_cache.get_method_version_cache()["method5"]
    assert catalog_cache.get_method_version_cache()["method5"]["release"]
