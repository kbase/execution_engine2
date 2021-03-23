"""
Unit tests for the job requirements resolver.
"""

from enum import Enum
from io import StringIO
from pytest import raises
from unittest.mock import create_autospec
from execution_engine2.sdk.job_submission_parameters import JobRequirements
from execution_engine2.utils.job_requirements_resolver import (
    JobRequirementsResolver,
    RequirementsType,
)
from execution_engine2.exceptions import IncorrectParamsException
from installed_clients.CatalogClient import Catalog
from utils_shared.test_utils import assert_exception_correct


def test_normalize_job_reqs_minimal():
    assert JobRequirementsResolver.normalize_job_reqs(None, "mysource") == {}
    assert JobRequirementsResolver.normalize_job_reqs({}, "mysource") == {}
    assert (
        JobRequirementsResolver.normalize_job_reqs(
            {
                "request_cpus": None,
                "request_memory": None,
                "request_disk": None,
                "client_group": None,
                "client_group_regex": None,
                "debug_mode": None,
                "expect_noop": " fooo  ",
            },
            "source",
        )
        == {}
    )
    assert (
        JobRequirementsResolver.normalize_job_reqs(
            {
                "request_cpus": "   \t   ",
                "request_memory": "   \t   ",
                "request_disk": "   \t   ",
                "client_group": "    \t    ",
                "client_group_regex": "   \t   ",
                "debug_mode": "   \t   ",
                "expect_noop": " fooo  ",
            },
            "source",
        )
        == {}
    )


def test_normalize_job_reqs_minimal_require_all():
    assert JobRequirementsResolver.normalize_job_reqs(
        {
            "request_cpus": 1,
            "request_memory": 1,
            "request_disk": 1,
            "client_group": "foo",
        },
        "source",
        True,
    ) == {
        "request_cpus": 1,
        "request_memory": 1,
        "request_disk": 1,
        "client_group": "foo",
    }


def test_normalize_job_reqs_maximal_ints():
    assert JobRequirementsResolver.normalize_job_reqs(
        {
            "request_cpus": 56,
            "request_memory": 200,
            "request_disk": 7000,
            "client_group": "     njs    ",
            "client_group_regex": 1,
            "debug_mode": -1,
            "expect_noop": 1,
        },
        "mysource",
    ) == {
        "request_cpus": 56,
        "request_memory": 200,
        "request_disk": 7000,
        "client_group": "njs",
        "client_group_regex": True,
        "debug_mode": True,
    }


def test_normalize_job_reqs_maximal_strings():
    assert JobRequirementsResolver.normalize_job_reqs(
        {
            "request_cpus": "   56   ",
            "request_memory": "   201     ",
            "request_disk": "    \t   7000    ",
            "client_group": "     njs    ",
            "client_group_regex": "    False    ",
            "debug_mode": "     true   \t   ",
            "expect_noop": 1,
        },
        "mysource",
    ) == {
        "request_cpus": 56,
        "request_memory": 201,
        "request_disk": 7000,
        "client_group": "njs",
        "client_group_regex": False,
        "debug_mode": True,
    }


def test_normalize_job_reqs_memory():
    for mem in [2000, "2000    ", "   2000M   ", "2000MB"]:
        assert JobRequirementsResolver.normalize_job_reqs(
            {"request_memory": mem}, "s"
        ) == {"request_memory": 2000}


def test_normalize_job_reqs_disk():
    for disk in [6000, "6000", "   6000GB   "]:
        assert JobRequirementsResolver.normalize_job_reqs(
            {"request_disk": disk}, "s"
        ) == {"request_disk": 6000}


def test_normalize_job_reqs_bools_true():
    for b in [True, 1, -1, 100, -100, "    True   ", "   true"]:
        assert JobRequirementsResolver.normalize_job_reqs(
            {"client_group_regex": b, "debug_mode": b}, "s"
        ) == {"client_group_regex": True, "debug_mode": True}


def test_normalize_job_reqs_bools_False():
    for b in [False, 0, "    False   ", "   false"]:
        assert JobRequirementsResolver.normalize_job_reqs(
            {"client_group_regex": b, "debug_mode": b}, "s"
        ) == {"client_group_regex": False, "debug_mode": False}


def test_normalize_job_reqs_fail_client_group():
    _normalize_job_reqs_fail(
        {"client_group": []},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal client group '[]' in job requirements from src"
        ),
    )
    _normalize_job_reqs_fail(
        {"client_group": "njs=true"},
        "src2",
        False,
        IncorrectParamsException(
            "Found illegal client group 'njs=true' in job requirements from src2"
        ),
    )


def test_normalize_job_reqs_fail_cpu():
    _normalize_job_reqs_fail(
        {"request_cpus": 8.4},
        "src3",
        False,
        IncorrectParamsException(
            "Found illegal cpu request '8.4' in job requirements from src3"
        ),
    )
    _normalize_job_reqs_fail(
        {"request_cpus": "26M"},
        "src4",
        False,
        IncorrectParamsException(
            "Found illegal cpu request '26M' in job requirements from src4"
        ),
    )


def test_normalize_job_reqs_fail_mem():
    _normalize_job_reqs_fail(
        {"request_memory": 3.2},
        "src5",
        False,
        IncorrectParamsException(
            "Found illegal memory request '3.2' in job requirements from src5"
        ),
    )
    _normalize_job_reqs_fail(
        {"request_memory": {}},
        "src5",
        False,
        IncorrectParamsException(
            "Found illegal memory request '{}' in job requirements from src5"
        ),
    )
    _normalize_job_reqs_fail(
        {"request_memory": "26G"},
        "src6",
        False,
        IncorrectParamsException(
            "Found illegal memory request '26G' in job requirements from src6"
        ),
    )


def test_normalize_job_reqs_fail_disk():
    _normalize_job_reqs_fail(
        {"request_disk": 6.5},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal disk request '6.5' in job requirements from src"
        ),
    )
    _normalize_job_reqs_fail(
        {"request_disk": set()},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal disk request 'set()' in job requirements from src"
        ),
    )
    _normalize_job_reqs_fail(
        {"request_disk": "26M"},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal disk request '26M' in job requirements from src"
        ),
    )


def test_normalize_job_reqs_fail_regex():
    _normalize_job_reqs_fail(
        {"client_group_regex": 92.4},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal client group regex '92.4' in job requirements from src"
        ),
    )
    _normalize_job_reqs_fail(
        {"client_group_regex": Enum},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal client group regex '<enum 'Enum'>' in job requirements from src"
        ),
    )
    _normalize_job_reqs_fail(
        {"client_group_regex": "truthy"},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal client group regex 'truthy' in job requirements from src"
        ),
    )


def test_normalize_job_reqs_fail_debug():
    _normalize_job_reqs_fail(
        {"debug_mode": 9.5},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal debug mode '9.5' in job requirements from src"
        ),
    )
    _normalize_job_reqs_fail(
        {"debug_mode": int},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal debug mode '<class 'int'>' in job requirements from src"
        ),
    )
    _normalize_job_reqs_fail(
        {"debug_mode": " yep "},
        "src",
        False,
        IncorrectParamsException(
            "Found illegal debug mode ' yep ' in job requirements from src"
        ),
    )


def test_normalize_job_reqs_fail_require_all():
    reqs_all = {
        "request_cpus": 56,
        "request_memory": 200,
        "request_disk": 7000,
        "client_group": "njs",
    }
    for k in ["request_cpus", "request_memory", "request_disk", "client_group"]:
        r = dict(reqs_all)
        del r[k]
        _normalize_job_reqs_fail(
            r,
            "mysrc",
            True,
            IncorrectParamsException(f"Missing {k} key in job requirements from mysrc"),
        )


def _normalize_job_reqs_fail(reqs, source, req_all_res, expected):
    with raises(Exception) as got:
        JobRequirementsResolver.normalize_job_reqs(reqs, source, req_all_res)
    assert_exception_correct(got.value, expected)


def test_get_requirements_type_standard():
    grt = JobRequirementsResolver.get_requirements_type
    assert grt() == RequirementsType.STANDARD
    assert (
        grt(None, None, None, None, None, None, None, None, None)
        == RequirementsType.STANDARD
    )
    assert (
        grt(None, None, None, None, None, None, False, {}, False)
        == RequirementsType.STANDARD
    )


def test_get_requirements_type_processing():
    grt = JobRequirementsResolver.get_requirements_type
    assert grt(cpus=4) == RequirementsType.PROCESSING
    assert grt(memory_MB=26) == RequirementsType.PROCESSING
    assert grt(disk_GB=78) == RequirementsType.PROCESSING
    assert grt(client_group="foo") == RequirementsType.PROCESSING
    assert grt(client_group_regex=False) == RequirementsType.PROCESSING
    assert grt(client_group_regex=True) == RequirementsType.PROCESSING
    assert grt(ignore_concurrency_limits=True) == RequirementsType.PROCESSING
    assert grt(scheduler_requirements={"a": "b"}) == RequirementsType.PROCESSING
    assert grt(debug_mode=True) == RequirementsType.PROCESSING

    assert (
        grt(
            cpus=4,
            memory_MB=2,
            disk_GB=8,
            client_group="yay",
            client_group_regex=True,
            ignore_concurrency_limits=True,
            debug_mode=True,
        )
        == RequirementsType.PROCESSING
    )


def test_get_requirements_type_billing():
    grt = JobRequirementsResolver.get_requirements_type
    assert grt(bill_to_user="foo") == RequirementsType.BILLING

    assert (
        grt(
            cpus=4,
            memory_MB=2,
            disk_GB=8,
            client_group="yay",
            client_group_regex=True,
            bill_to_user="can I buy you a drink?",
            ignore_concurrency_limits=True,
            debug_mode=True,
        )
        == RequirementsType.BILLING
    )


def test_get_requirements_type_fail():
    # All the illegal requirements testing is delegated to a method outside the code
    # unit under test, so we just do one test per input to be sure it's hooked up correctly
    # and delegate more thorough testing to the unit tests for the called method.
    n = None
    _grtf = _get_requirements_type_fail
    _grtf(0, n, n, n, n, IncorrectParamsException("CPU count must be at least 1"))
    _grtf(n, 0, n, n, n, IncorrectParamsException("memory in MB must be at least 1"))
    _grtf(
        n, n, 0, n, n, IncorrectParamsException("disk space in GB must be at least 1")
    )
    _grtf(
        n,
        n,
        n,
        "    \t   ",
        n,
        IncorrectParamsException("Missing input parameter: client_group"),
    )
    _grtf(
        n,
        n,
        n,
        n,
        "   \bfoo   ",
        IncorrectParamsException("bill_to_user contains control characters"),
    )
    # note there are no invalid values for client_group_regex, ignore_concurrentcy_limits,
    # and debug_mode


def _get_requirements_type_fail(cpus, mem, disk, cg, btu, expected):
    with raises(Exception) as got:
        JobRequirementsResolver.get_requirements_type(
            cpus, mem, disk, cg, False, btu, False, False
        )
    assert_exception_correct(got.value, expected)


def _get_simple_deploy_spec_file_obj():
    return StringIO(
        """
        [execution_engine2]
        request_cpus = 0
        request_memory = 2000M
        request_disk = 100GB

        [DEFAULT]
        default_client_group =   cg2

        [cg1]
        request_cpus = 4
        request_memory = 2000M
        request_disk = 100GB

        [cg2]
        request_cpus = 8
        request_memory = 700
        request_disk = 32
        debug_mode = True
        client_group_regex = false
        """
    )


# Note the constructor uses the normalization class method under the hood for normalizing
# the EE2 config file client groups. As such, we don't duplicate the testing of that method
# here other than some spot checks. If the constructor changes significantly more
# testing may be required.


def test_init():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)

    spec = _get_simple_deploy_spec_file_obj()

    jrr = JobRequirementsResolver(catalog, spec)
    assert jrr.get_default_client_group() == "cg2"
    assert jrr.get_override_client_group() is None
    assert jrr.get_configured_client_groups() == set(["cg1", "cg2"])
    assert jrr.get_configured_client_group_spec("cg1") == {
        "request_cpus": 4,
        "request_memory": 2000,
        "request_disk": 100,
        "client_group": "cg1",
    }

    assert jrr.get_configured_client_group_spec("cg2") == {
        "request_cpus": 8,
        "request_memory": 700,
        "request_disk": 32,
        "client_group": "cg2",
        "debug_mode": True,
        "client_group_regex": False,
    }


def test_init_with_override():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)

    spec = _get_simple_deploy_spec_file_obj()
    jrr = JobRequirementsResolver(catalog, spec, "  \t   ")
    assert jrr.get_override_client_group() is None

    spec = _get_simple_deploy_spec_file_obj()
    jrr = JobRequirementsResolver(catalog, spec, "cg1")
    assert jrr.get_override_client_group() == "cg1"


def test_init_fail_missing_input():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    _init_fail(
        None,
        _get_simple_deploy_spec_file_obj(),
        None,
        ValueError("catalog cannot be a value that evaluates to false"),
    )
    _init_fail(
        catalog,
        None,
        None,
        ValueError("cfgfile cannot be a value that evaluates to false"),
    )
    _init_fail(
        catalog,
        [],
        None,
        ValueError("cfgfile cannot be a value that evaluates to false"),
    )


def test_init_fail_no_override_in_config():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)

    spec = _get_simple_deploy_spec_file_obj()
    _init_fail(
        catalog,
        spec,
        "cg3",
        ValueError("No deployment configuration entry for override client group 'cg3'"),
    )


def test_init_fail_default_config_error():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)

    shared_spec = """
        [njs]
        request_cpus = 4
        request_memory = 2000M
        request_disk = 100GB
        """

    _init_fail(
        catalog,
        StringIO(shared_spec),
        None,
        IncorrectParamsException(
            "Missing input parameter: value for DEFAULT.default_client_group in deployment "
            + "config file"
        ),
    )

    spec = StringIO(
        shared_spec
        + """
        [DEFAULT]
        foo = bar
        """
    )
    _init_fail(
        catalog,
        spec,
        None,
        IncorrectParamsException(
            "Missing input parameter: value for DEFAULT.default_client_group in deployment "
            + "config file"
        ),
    )

    spec = StringIO(
        shared_spec
        + """
        [DEFAULT]
        default_client_group = njrs
        """
    )
    _init_fail(
        catalog,
        spec,
        None,
        ValueError("No deployment configuration entry for default client group 'njrs'"),
    )


def test_init_fail_bad_config():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)

    shared_spec = """
        [DEFAULT]
        default_client_group = njs
        """

    spec = (
        shared_spec
        + """
        [njs]
        request_memory = 2000M
        request_disk = 100GB
        """
    )

    _init_fail(
        catalog,
        StringIO(spec),
        None,
        IncorrectParamsException(
            "Missing request_cpus key in job requirements from section 'njs' of the "
            + "deployment configuration"
        ),
    )

    spec = (
        shared_spec
        + """
        [njs]
        request_cpus = 4
        request_disk = 100GB
        """
    )

    _init_fail(
        catalog,
        StringIO(spec),
        None,
        IncorrectParamsException(
            "Missing request_memory key in job requirements from section 'njs' of the "
            + "deployment configuration"
        ),
    )

    spec = (
        shared_spec
        + """
        [njs]
        request_cpus = 4
        request_memory = 2000M
        """
    )

    _init_fail(
        catalog,
        StringIO(spec),
        None,
        IncorrectParamsException(
            "Missing request_disk key in job requirements from section 'njs' of the "
            + "deployment configuration"
        ),
    )


def _init_fail(catalog, spec, override, expected):
    with raises(Exception) as got:
        JobRequirementsResolver(catalog, spec, override)
    assert_exception_correct(got.value, expected)


def test_get_configured_client_group_spec_fail():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)

    jrr = JobRequirementsResolver(catalog, _get_simple_deploy_spec_file_obj())

    with raises(Exception) as got:
        jrr.get_configured_client_group_spec("cg4")
    assert_exception_correct(
        got.value, ValueError("Client group 'cg4' is not configured")
    )


# Note that resolve_requirements uses the normalization class method and an argument checking
# method under the hood. As such, we don't duplicate the testing of those methods
# here other than some spot checks. If the method changes significantly more
# testing may be required.


def test_resolve_requirements_from_spec():
    """
    Resolve requirements when no user input and no catalog record is available.
    """
    _resolve_requirements_from_spec([])
    _resolve_requirements_from_spec([{}])
    _resolve_requirements_from_spec([{'client_groups': []}])


def _resolve_requirements_from_spec(catalog_return):
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = catalog_return

    spec = _get_simple_deploy_spec_file_obj()

    jrr = JobRequirementsResolver(catalog, spec)

    assert jrr.resolve_requirements(" mod.meth  ") == JobRequirements(
        8,
        700,
        32,
        'cg2',
        client_group_regex=False,
        debug_mode=True,
    )

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "mod",
        "function_name": "meth"
    })


def test_resolve_requirements_from_spec_with_override():
    """
    Test that an override ignores client group information from all other sources.
    """
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{'client_groups': ["cg2"]}]

    spec = _get_simple_deploy_spec_file_obj()

    jrr = JobRequirementsResolver(catalog, spec, "    cg1    ")

    assert jrr.resolve_requirements(
        " module2. some_meth  ", client_group="cg2") == JobRequirements(
            4,
            2000,
            100,
            'cg1',
    )

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "module2",
        "function_name": "some_meth"
    })


def test_resolve_requirements_from_catalog_full_CSV():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{"client_groups": [
        "cg1",
        "request_cpus=  78",
        "   request_memory   = 500MB",
        "request_disk = 700GB",
        "client_group_regex = False",
        "debug_mode = true",
        "foo=bar",
        "baz=bat",
    ]}]

    spec = _get_simple_deploy_spec_file_obj()

    jrr = JobRequirementsResolver(catalog, spec)

    assert jrr.resolve_requirements(" module2. some_meth  ") == JobRequirements(
        78,
        500,
        700,
        'cg1',
        False,
        None,
        False,
        {"foo": "bar", "baz": "bat"},
        True,
    )

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "module2",
        "function_name": "some_meth"
    })


def test_resolve_requirements_from_catalog_partial_JSON():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{"client_groups": [
        '{"client_group": "  cg1  "',
        '"    request_memory    ":  "  300M   "',
        '"exactlythesameshape": "asathingy"',
        '"request_disk": 100000}',
    ]}]

    spec = _get_simple_deploy_spec_file_obj()

    jrr = JobRequirementsResolver(catalog, spec)

    assert jrr.resolve_requirements(" module2. some_meth  ") == JobRequirements(
        4,
        300,
        100000,
        'cg1',
        scheduler_requirements={"exactlythesameshape": "asathingy"}
    )

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "module2",
        "function_name": "some_meth"
    })


def test_resolve_requirements_from_user_full():
    _resolve_requirements_from_user_full(True)
    _resolve_requirements_from_user_full(False)


def _resolve_requirements_from_user_full(bool_val):
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{"client_groups": [
        "cg2",
        "request_cpus=  78",
        "   request_memory   = 500MB",
        "request_disk = 700GB",
        "client_group_regex = False",
        "debug_mode = true",
        "foo=bar",
        "baz=bat",
    ]}]

    spec = _get_simple_deploy_spec_file_obj()

    jrr = JobRequirementsResolver(catalog, spec)

    assert jrr.resolve_requirements(
        " module2. some_meth  ",
        42,
        789,
        1,
        "cg1",
        bool_val,
        "some_poor_sucker",
        bool_val,
        {"foo": "Some of you may die", "bar": "but that is a sacrifice I am willing to make"},
        bool_val,
    ) == JobRequirements(
        42,
        789,
        1,
        'cg1',
        bool_val,
        "some_poor_sucker",
        bool_val,
        {"foo": "Some of you may die",
         "bar": "but that is a sacrifice I am willing to make",
         "baz": "bat"},
        bool_val,
    )

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "module2",
        "function_name": "some_meth"
    })


def test_resolve_requirements_from_user_partial():
    """
    Gets requirements from the user, catalog, and the ee2 deploy config.

    Also tests that special keys are removed from the scheduler requirements.
    """
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{"client_groups": [
        "cg2",
        "request_cpus=  78",
        "request_disk = 700",
        "client_group_regex = False",
        "debug_mode = true",
        "foo=bar",
        "baz=bat",
    ]}]

    spec = _get_simple_deploy_spec_file_obj()

    jrr = JobRequirementsResolver(catalog, spec)

    assert jrr.resolve_requirements(
        " module2. some_meth  ",
        cpus=42,
        client_group="cg1",
        client_group_regex=True,
        scheduler_requirements={
            "client_group": "foo",
            "request_cpus": "78",
            "request_memory": "800",
            "request_disk": "700",
            "client_group_regex": "False",
            "debug_mode": "True",
            "bill_to_user": "foo",
            "ignore_concurrency_limits": "true",
            "whee": "whoo",
        }
    ) == JobRequirements(
        42,
        2000,
        700,
        'cg1',
        client_group_regex=True,
        scheduler_requirements={"foo": "bar", "baz": "bat", "whee": "whoo"},
        debug_mode=True,
    )

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "module2",
        "function_name": "some_meth"
    })


def test_resolve_requirements_fail_illegal_inputs():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    jrr = JobRequirementsResolver(catalog, _get_simple_deploy_spec_file_obj())

    _resolve_requirements_fail(jrr, None, {}, IncorrectParamsException(
        "Unrecognized method: 'None'. Please input module_name.function_name"))
    _resolve_requirements_fail(jrr, "method", {}, IncorrectParamsException(
        "Unrecognized method: 'method'. Please input module_name.function_name"))
    _resolve_requirements_fail(jrr, "mod1.mod2.method", {}, IncorrectParamsException(
        "Unrecognized method: 'mod1.mod2.method'. Please input module_name.function_name"))
    _resolve_requirements_fail(jrr, "m.m", {"cpus": 0}, IncorrectParamsException(
        "CPU count must be at least 1"))
    _resolve_requirements_fail(jrr, "m.m", {"memory_MB": 0}, IncorrectParamsException(
        "memory in MB must be at least 1"))
    _resolve_requirements_fail(jrr, "m.m", {"disk_GB": 0}, IncorrectParamsException(
        "disk space in GB must be at least 1"))
    _resolve_requirements_fail(jrr, "m.m", {"client_group": "   \t   "}, IncorrectParamsException(
        "Missing input parameter: client_group"))
    _resolve_requirements_fail(jrr, "m.m", {"bill_to_user": "\b"}, IncorrectParamsException(
        "bill_to_user contains control characters"))
    _resolve_requirements_fail(
        jrr, "m.m", {"scheduler_requirements": {"a": None}}, IncorrectParamsException(
            "Missing input parameter: value for key 'a' in scheduler requirements structure"))


def test_resolve_requirements_fail_catalog_multiple_entries():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{'client_groups': ["cg2"]}, {}]

    jrr = JobRequirementsResolver(catalog, _get_simple_deploy_spec_file_obj())
    _resolve_requirements_fail(jrr, "m.m", {}, ValueError(
        "Unexpected result from the Catalog service: more than one client group "
        + "configuration found for method m.m"))

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "m",
        "function_name": "m"
    })


def test_resolve_requirements_fail_catalog_bad_JSON():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{'client_groups': [
        '{"foo": "bar", "baz":}']}]

    jrr = JobRequirementsResolver(catalog, _get_simple_deploy_spec_file_obj())
    _resolve_requirements_fail(jrr, "m.m", {}, ValueError(
        "Unable to parse JSON client group entry from catalog for method m.m"))

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "m",
        "function_name": "m"
    })


def test_resolve_requirements_fail_catalog_bad_CSV():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{'client_groups': [
        "cg", "foo is bar"]}]

    jrr = JobRequirementsResolver(catalog, _get_simple_deploy_spec_file_obj())
    _resolve_requirements_fail(jrr, "m.m", {}, ValueError(
        "Malformed requirement. Format is <key>=<value>. "
        + "Item is 'foo is bar' for catalog method m.m"))

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "m",
        "function_name": "m"
    })


def test_resolve_requirements_fail_catalog_normalize():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{'client_groups': [
        "cg", "request_memory=72TB"]}]

    jrr = JobRequirementsResolver(catalog, _get_simple_deploy_spec_file_obj())
    _resolve_requirements_fail(jrr, " mod  . meth ", {}, IncorrectParamsException(
        "Found illegal memory request '72TB' in job requirements from catalog method mod.meth"))

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "mod",
        "function_name": "meth"
    })


def test_resolve_requirements_fail_catalog_clientgroup():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = [{'client_groups': [
        "cg", "request_memory=72"]}]

    jrr = JobRequirementsResolver(catalog, _get_simple_deploy_spec_file_obj())
    _resolve_requirements_fail(jrr, " mod  . meth ", {}, IncorrectParamsException(
        "Catalog specified illegal client group 'cg' for method mod.meth"))

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "mod",
        "function_name": "meth"
    })


def test_resolve_requirements_fail_input_clientgroup():
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catalog.list_client_group_configs.return_value = []

    jrr = JobRequirementsResolver(catalog, _get_simple_deploy_spec_file_obj())
    _resolve_requirements_fail(jrr, "m.m", {"client_group": "cb4"}, IncorrectParamsException(
        "No such clientgroup: cb4"))

    catalog.list_client_group_configs.assert_called_once_with({
        "module_name": "m",
        "function_name": "m"
    })


def _resolve_requirements_fail(jrr, method, kwargs, expected):
    with raises(Exception) as got:
        jrr.resolve_requirements(method, **kwargs)
    assert_exception_correct(got.value, expected)
