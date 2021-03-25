"""
Unit tests for the job requirements resolver.
"""

from enum import Enum
from pytest import raises
from execution_engine2.utils.job_requirements_resolver import JobRequirementsResolver
from execution_engine2.exceptions import IncorrectParamsException
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
