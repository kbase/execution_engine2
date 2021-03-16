from pytest import raises
from execution_engine2.sdk.job_submission_parameters import JobRequirements
from execution_engine2.exceptions import IncorrectParamsException
from utils_shared.test_utils import assert_exception_correct


def test_job_req_init_minimal():
    jr = JobRequirements(1, 1, 1, "njs")

    assert jr.cpus == 1
    assert jr.memory_MB == 1
    assert jr.disk_GB == 1
    assert jr.client_group == "njs"
    assert jr.client_group_regex is False
    assert jr.as_user is None
    assert jr.ignore_concurrency_limits is False
    assert jr.scheduler_requirements == {}


def test_job_req_init_maximal():
    jr = JobRequirements(
        6,
        7,
        8,
        "bigmemlong",
        True,
        "someuser",
        True,
        {"proc": "x286", "maxmem": "640k"},
    )

    assert jr.cpus == 6
    assert jr.memory_MB == 7
    assert jr.disk_GB == 8
    assert jr.client_group == "bigmemlong"
    assert jr.client_group_regex is True
    assert jr.as_user == "someuser"
    assert jr.ignore_concurrency_limits is True
    assert jr.scheduler_requirements == {"proc": "x286", "maxmem": "640k"}


def test_job_req_init_fail():
    n = None
    _job_req_init_fail(
        n, 1, 1, "f", n, n, IncorrectParamsException("CPU count must be at least 1")
    )
    _job_req_init_fail(
        0, 1, 1, "f", n, n, IncorrectParamsException("CPU count must be at least 1")
    )
    _job_req_init_fail(
        1, n, 1, "f", n, n, IncorrectParamsException("memory in MB must be at least 1")
    )
    _job_req_init_fail(
        1, 0, 1, "f", n, n, IncorrectParamsException("memory in MB must be at least 1")
    )
    _job_req_init_fail(
        1,
        1,
        n,
        "f",
        n,
        n,
        IncorrectParamsException("disk space in GB must be at least 1"),
    )
    _job_req_init_fail(
        1,
        1,
        0,
        "f",
        n,
        n,
        IncorrectParamsException("disk space in GB must be at least 1"),
    )
    _job_req_init_fail(
        1,
        1,
        1,
        n,
        n,
        n,
        IncorrectParamsException("Missing input parameter: client_group"),
    )
    _job_req_init_fail(
        1,
        1,
        1,
        "  \t    ",
        n,
        n,
        IncorrectParamsException("Missing input parameter: client_group"),
    )
    # as_user is optional, so this is the only possible failure mode
    _job_req_init_fail(
        1,
        1,
        1,
        "f",
        "user\tname",
        n,
        IncorrectParamsException("as_user contains control characters"),
    )
    _job_req_init_fail(
        1,
        1,
        1,
        "f",
        n,
        {n: "a"},
        IncorrectParamsException(
            "Missing input parameter: key in scheduler requirements structure"
        ),
    )
    _job_req_init_fail(
        1,
        1,
        1,
        "f",
        n,
        {"    \t   ": "a"},
        IncorrectParamsException(
            "Missing input parameter: key in scheduler requirements structure"
        ),
    )
    _job_req_init_fail(
        1,
        1,
        1,
        "f",
        n,
        {"a": n},
        IncorrectParamsException(
            "Missing input parameter: value for key 'a' in scheduler requirements structure"
        ),
    )
    _job_req_init_fail(
        1,
        1,
        1,
        "f",
        n,
        {"a": "    \t   "},
        IncorrectParamsException(
            "Missing input parameter: value for key 'a' in scheduler requirements structure"
        ),
    )


def _job_req_init_fail(cpus, mem, disk, cgroup, user, reqs, expected):
    with raises(Exception) as got:
        JobRequirements(cpus, mem, disk, cgroup, False, user, False, reqs)
    assert_exception_correct(got.value, expected)


def test_equals():
    c1 = "cligroupf"
    c1a = "cligroupf"
    c2 = "cligroupg"
    t = True
    f = False
    u1 = "user1"
    u1a = "user1"
    u2 = "user2"
    r1 = {"a": "b"}
    r1a = {"a": "b"}
    r2 = {"a": "c"}

    assert JobRequirements(1, 1, 1, c1) == JobRequirements(1, 1, 1, c1a)
    assert JobRequirements(1, 1, 1, c1, t, u1, f, r1) == JobRequirements(
        1, 1, 1, c1a, t, u1a, f, r1a
    )

    assert JobRequirements(1, 1, 1, c1) != JobRequirements(2, 1, 1, c1a)
    assert JobRequirements(1, 1, 1, c1) != JobRequirements(1, 2, 1, c1a)
    assert JobRequirements(1, 1, 1, c1) != JobRequirements(1, 1, 2, c1a)
    assert JobRequirements(1, 1, 1, c1) != JobRequirements(1, 1, 1, c2)
    assert JobRequirements(1, 1, 1, c1) != (1, 1, 1, c1)

    assert JobRequirements(1, 1, 1, c1, t, u1, f, r1) != JobRequirements(
        1, 1, 1, c1a, f, u1a, f, r1a
    )
    assert JobRequirements(1, 1, 1, c1, t, u1, f, r1) != JobRequirements(
        1, 1, 1, c1a, t, u2, f, r1a
    )
    assert JobRequirements(1, 1, 1, c1, t, u1, f, r1) != JobRequirements(
        1, 1, 1, c1a, t, u1a, t, r1a
    )
    assert JobRequirements(1, 1, 1, c1, t, u1, f, r1) != JobRequirements(
        1, 1, 1, c1a, t, u1a, f, r2
    )
    assert JobRequirements(1, 1, 1, c1, t, u1, f, r1) != (1, 1, 1, c1a, t, u1a, f, r1a)


def test_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    c1 = "cligroupf"
    c1a = "cligroupf"
    c2 = "cligroupg"
    t = True
    f = False
    u1 = "user1"
    u1a = "user1"
    u2 = "user2"
    r1 = {"a": "b"}
    r1a = {"a": "b"}
    r2 = {"a": "c"}

    assert hash(JobRequirements(1, 1, 1, c1)) == hash(JobRequirements(1, 1, 1, c1a))
    assert hash(JobRequirements(1, 1, 1, c1, t, u1, f, r1)) == hash(
        JobRequirements(1, 1, 1, c1a, t, u1a, f, r1a)
    )

    assert hash(JobRequirements(1, 1, 1, c1)) != hash(JobRequirements(2, 1, 1, c1a))
    assert hash(JobRequirements(1, 1, 1, c1)) != hash(JobRequirements(1, 2, 1, c1a))
    assert hash(JobRequirements(1, 1, 1, c1)) != hash(JobRequirements(1, 1, 2, c1a))
    assert hash(JobRequirements(1, 1, 1, c1)) != hash(JobRequirements(1, 1, 1, c2))

    assert hash(JobRequirements(1, 1, 1, c1, t, u1, f, r1)) != hash(
        JobRequirements(1, 1, 1, c1a, f, u1a, f, r1a)
    )
    assert hash(JobRequirements(1, 1, 1, c1, t, u1, f, r1)) != hash(
        JobRequirements(1, 1, 1, c1a, t, u2, f, r1a)
    )
    assert hash(JobRequirements(1, 1, 1, c1, t, u1, f, r1)) != hash(
        JobRequirements(1, 1, 1, c1a, t, u1a, t, r1a)
    )
    assert hash(JobRequirements(1, 1, 1, c1, t, u1, f, r1)) != hash(
        JobRequirements(1, 1, 1, c1a, t, u1a, f, r2)
    )
