from pytest import raises
from execution_engine2.sdk.job_submission_parameters import (
    JobRequirements,
    JobSubmissionParameters,
)
from execution_engine2.utils.user_info import UserCreds
from execution_engine2.utils.application_info import AppInfo
from execution_engine2.exceptions import IncorrectParamsException
from utils_shared.test_utils import assert_exception_correct


def test_job_req_init_minimal():
    jr = JobRequirements(1, 1, 1, "njs")

    assert jr.cpus == 1
    assert jr.memory_MB == 1
    assert jr.disk_GB == 1
    assert jr.client_group == "njs"
    assert jr.client_group_regex is None
    assert jr.as_user is None
    assert jr.ignore_concurrency_limits is False
    assert jr.scheduler_requirements == {}
    assert jr.debug_mode is False


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
        True,
    )

    assert jr.cpus == 6
    assert jr.memory_MB == 7
    assert jr.disk_GB == 8
    assert jr.client_group == "bigmemlong"
    assert jr.client_group_regex is True
    assert jr.as_user == "someuser"
    assert jr.ignore_concurrency_limits is True
    assert jr.scheduler_requirements == {"proc": "x286", "maxmem": "640k"}
    assert jr.debug_mode is True


def test_job_req_init_non_bools():
    for inp, expected in {
            1: True, " ": True, (1,): True, 0: False, "": False, tuple(): False}.items():
        jr = JobRequirements(
            6,
            7,
            8,
            "cg",
            client_group_regex=inp,
            ignore_concurrency_limits=inp,
            debug_mode=inp)

        assert jr.client_group_regex is expected
        assert jr.ignore_concurrency_limits is expected
        assert jr.debug_mode is expected


def test_job_req_init_None_for_bools():
    jr = JobRequirements(
        6,
        7,
        8,
        "cg",
        client_group_regex=None,
        ignore_concurrency_limits=None,
        debug_mode=None)

    assert jr.client_group_regex is None
    assert jr.ignore_concurrency_limits is False
    assert jr.debug_mode is False


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


def test_job_req_check_parameters_no_input():
    # if no exception, success
    JobRequirements.check_parameters()
    n = None
    JobRequirements.check_parameters(n, n, n, n, n, n, n, n, n)


def test_job_req_check_parameters_full_input():
    JobRequirements.check_parameters(  # if no exception, success
        1,
        1,
        1,
        "   b   ",
        'x',
        " user ",
        890,
        {"proc": "x286", "maxmem": "640k"},
        [],
    )


def test_job_req_check_parameters_whitespace_as_user():
    JobRequirements.check_parameters(  # if no exception, success
        1,
        1,
        1,
        "   b   ",
        'x',
        " \t  ",
        890,
        {"proc": "x286", "maxmem": "640k"},
        [],
    )


def test_job_req_check_parameters_fail():
    n = None
    _job_req_check_parameters_fail(0, 1, 1, "c", "u", n, IncorrectParamsException(
        "CPU count must be at least 1"))
    _job_req_check_parameters_fail(1, 0, 1, "c", "u", n, IncorrectParamsException(
        "memory in MB must be at least 1"))
    _job_req_check_parameters_fail(1, 1, 0, "c", "u", n, IncorrectParamsException(
        "disk space in GB must be at least 1"))
    _job_req_check_parameters_fail(1, 1, 1, " \t ", "u", n, IncorrectParamsException(
        "Missing input parameter: client_group"))
    _job_req_check_parameters_fail(1, 1, 1, "c", "  j\bi  ", n, IncorrectParamsException(
        "as_user contains control characters"))
    _job_req_check_parameters_fail(1, 1, 1, "c", "u", {None: 1}, IncorrectParamsException(
        "Missing input parameter: key in scheduler requirements structure"))
    _job_req_check_parameters_fail(1, 1, 1, "c", "u", {"a": None}, IncorrectParamsException(
        "Missing input parameter: value for key 'a' in scheduler requirements structure"))
    _job_req_check_parameters_fail(1, 1, 1, "c", "u", {" \t ": 1}, IncorrectParamsException(
        "Missing input parameter: key in scheduler requirements structure"))
    _job_req_check_parameters_fail(1, 1, 1, "c", "u", {"b": " \t "}, IncorrectParamsException(
        "Missing input parameter: value for key 'b' in scheduler requirements structure"))


def _job_req_check_parameters_fail(cpu, mem, disk, cgroup, user, reqs, expected):
    with raises(Exception) as got:
        JobRequirements(cpu, mem, disk, cgroup, True, user, True, reqs)
    assert_exception_correct(got.value, expected)


def test_job_req_equals():
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

    jr_sm = JobRequirements(1, 1, 1, c1)
    jr_lg = JobRequirements(1, 1, 1, c1, t, u1, f, r1, t)

    assert jr_sm == JobRequirements(1, 1, 1, c1a)
    assert jr_lg == JobRequirements(1, 1, 1, c1a, t, u1a, f, r1a, t)

    assert jr_sm != JobRequirements(2, 1, 1, c1a)
    assert jr_sm != JobRequirements(1, 2, 1, c1a)
    assert jr_sm != JobRequirements(1, 1, 2, c1a)
    assert jr_sm != JobRequirements(1, 1, 1, c2)
    assert jr_sm != (1, 1, 1, c1)

    assert jr_lg != JobRequirements(1, 1, 1, c1a, f, u1a, f, r1a, t)
    assert jr_lg != JobRequirements(1, 1, 1, c1a, t, u2, f, r1a, t)
    assert jr_lg != JobRequirements(1, 1, 1, c1a, t, u1a, t, r1a, t)
    assert jr_lg != JobRequirements(1, 1, 1, c1a, t, u1a, f, r2, t)
    assert jr_lg != JobRequirements(1, 1, 1, c1a, t, u1a, f, r1a, f)
    assert jr_lg != (1, 1, 1, c1a, t, u1a, f, r1a, t)


def test_job_req_hash():
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

    jr_sm = JobRequirements(1, 1, 1, c1)
    jr_lg = JobRequirements(1, 1, 1, c1, t, u1, f, r1, t)

    assert hash(jr_sm) == hash(JobRequirements(1, 1, 1, c1a))
    assert hash(jr_lg) == hash(JobRequirements(1, 1, 1, c1a, t, u1a, f, r1a, t))

    assert hash(jr_sm) != hash(JobRequirements(2, 1, 1, c1a))
    assert hash(jr_sm) != hash(JobRequirements(1, 2, 1, c1a))
    assert hash(jr_sm) != hash(JobRequirements(1, 1, 2, c1a))
    assert hash(jr_sm) != hash(JobRequirements(1, 1, 1, c2))

    assert hash(jr_lg) != hash(JobRequirements(1, 1, 1, c1a, f, u1a, f, r1a, t))
    assert hash(jr_lg) != hash(JobRequirements(1, 1, 1, c1a, t, u2, f, r1a, t))
    assert hash(jr_lg) != hash(JobRequirements(1, 1, 1, c1a, t, u1a, t, r1a, t))
    assert hash(jr_lg) != hash(JobRequirements(1, 1, 1, c1a, t, u1a, f, r2, t))
    assert hash(jr_lg) != hash(JobRequirements(1, 1, 1, c1a, t, u1a, f, r1a, f))


def test_job_sub_init_minimal():
    jsp = JobSubmissionParameters(
        "jobid",
        AppInfo("a.b", "a/x"),
        JobRequirements(6, 7, 4, "cligroup"),
        UserCreds("user", "tokeytoken"),
    )

    assert jsp.job_id == "jobid"
    assert jsp.app_info == AppInfo("a.b", "a/x")
    assert jsp.job_reqs == JobRequirements(6, 7, 4, "cligroup")
    assert jsp.user_creds == UserCreds("user", "tokeytoken")
    assert jsp.parent_job_id is None
    assert jsp.wsid is None
    assert jsp.source_ws_objects == tuple()


def test_job_sub_init_maximal():
    jsp = JobSubmissionParameters(
        "    jobid  \t  ",
        AppInfo("a.b", "a/x"),
        JobRequirements(6, 7, 4, "cligroup"),
        UserCreds("user", "tokeytoken"),
        "    parentid  \t   ",
        1,
        ["   1   /\t2   /   4", "6/7/8"],
    )

    assert jsp.job_id == "jobid"
    assert jsp.app_info == AppInfo("a.b", "a/x")
    assert jsp.job_reqs == JobRequirements(6, 7, 4, "cligroup")
    assert jsp.user_creds == UserCreds("user", "tokeytoken")
    assert jsp.parent_job_id == "parentid"
    assert jsp.wsid == 1
    assert jsp.source_ws_objects == ("1/2/4", "6/7/8")


def test_job_sub_init_fail():
    n = None
    j = "jobby job job"
    a = AppInfo("a.b", "a/x")
    r = JobRequirements(6, 7, 4, "cligroup")
    u = UserCreds("user", "tokeytoken")

    _job_sub_init_fail(
        n, a, r, u, n, n, n, IncorrectParamsException("Missing input parameter: job_id")
    )
    _job_sub_init_fail(
        "  \t   ",
        a,
        r,
        u,
        n,
        n,
        n,
        IncorrectParamsException("Missing input parameter: job_id"),
    )
    _job_sub_init_fail(
        j,
        n,
        r,
        u,
        n,
        n,
        n,
        ValueError("app_info cannot be a value that evaluates to false"),
    )
    _job_sub_init_fail(
        j,
        a,
        n,
        u,
        n,
        n,
        n,
        ValueError("job_reqs cannot be a value that evaluates to false"),
    )
    _job_sub_init_fail(
        j,
        a,
        r,
        n,
        n,
        n,
        n,
        ValueError("user_creds cannot be a value that evaluates to false"),
    )
    # the only way to get parent id to to fail is with a control char
    _job_sub_init_fail(
        j,
        a,
        r,
        u,
        "par\bent",
        n,
        n,
        IncorrectParamsException("parent_job_id contains control characters"),
    )
    _job_sub_init_fail(
        j, a, r, u, n, 0, n, IncorrectParamsException("wsid must be at least 1")
    )
    _job_sub_init_fail(
        j,
        a,
        r,
        u,
        n,
        n,
        ["1/2/3", n],
        IncorrectParamsException(
            "source_ws_objects index 1, 'None', is not a valid Unique Permanent Address"
        ),
    )
    _job_sub_init_fail(
        j,
        a,
        r,
        u,
        n,
        n,
        ["1/2/3", "   \t  "],
        IncorrectParamsException(
            "source_ws_objects index 1, '   \t  ', is not a valid Unique Permanent Address"
        ),
    )
    for o in ["1/2", "1/2/", "/1/2", "1/2/3/4", "x/2/3", "1/x/3", "1/2/x"]:
        _job_sub_init_fail(
            j,
            a,
            r,
            u,
            n,
            n,
            [o],
            IncorrectParamsException(
                f"source_ws_objects index 0, '{o}', is not a valid Unique Permanent Address"
            ),
        )


def _job_sub_init_fail(jobid, appinfo, jobreq, usercred, parentid, wsid, wso, expected):
    with raises(Exception) as got:
        JobSubmissionParameters(jobid, appinfo, jobreq, usercred, parentid, wsid, wso)
    assert_exception_correct(got.value, expected)


def test_job_sub_equals():
    j1 = "jobby job job"
    j1a = "jobby job job"
    j2 = "jobby job job JOB"
    a1 = AppInfo("a.b", "a/x")
    a1a = AppInfo("a.b", "a/x")
    a2 = AppInfo("a.b", "a/y")
    r1 = JobRequirements(6, 7, 4, "cligroup")
    r1a = JobRequirements(6, 7, 4, "cligroup")
    r2 = JobRequirements(6, 7, 4, "cligroup2")
    u1 = UserCreds("user", "tokeytoken")
    u1a = UserCreds("user", "tokeytoken")
    u2 = UserCreds("user", "tokeytoken2")
    p1 = "I'm so miserable and you just don't care"
    p1a = "I'm so miserable and you just don't care"
    p2 = "Oh do shut up Portia"
    w1 = ["1/2/3"]
    w1a = ["1/2/3"]
    w2 = ["1/2/4"]

    JSP = JobSubmissionParameters
    jsp_sm = JSP(j1, a1, r1, u1)
    jsp_lg = JSP(j1, a1, r1, u1, p1, 1, w1)

    assert jsp_sm == JSP(j1a, a1a, r1a, u1a)
    assert jsp_lg == JSP(j1a, a1a, r1a, u1a, p1a, 1, w1a)

    assert jsp_sm != JSP(j2, a1a, r1a, u1a)
    assert jsp_sm != JSP(j1a, a2, r1a, u1a)
    assert jsp_sm != JSP(j1a, a1a, r2, u1a)
    assert jsp_sm != JSP(j1a, a1a, r1a, u2)
    assert jsp_sm != (j1a, a1a, r1a, u1a)

    assert jsp_lg != JSP(j1a, a1a, r1a, u1a, p2, 1, w1a)
    assert jsp_lg != JSP(j1a, a1a, r1a, u1a, p1a, 2, w1a)
    assert jsp_lg != JSP(j1a, a1a, r1a, u1a, p1a, 1, w2)
    assert jsp_lg != (j1a, a1a, r1a, u1a, p1a, 1, w1a)


def test_job_sub_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    j1 = "jobby job job"
    j1a = "jobby job job"
    j2 = "jobby job job JOB"
    a1 = AppInfo("a.b", "a/x")
    a1a = AppInfo("a.b", "a/x")
    a2 = AppInfo("a.b", "a/y")
    r1 = JobRequirements(6, 7, 4, "cligroup")
    r1a = JobRequirements(6, 7, 4, "cligroup")
    r2 = JobRequirements(6, 7, 4, "cligroup2")
    u1 = UserCreds("user", "tokeytoken")
    u1a = UserCreds("user", "tokeytoken")
    u2 = UserCreds("user", "tokeytoken2")
    p1 = "I'm so miserable and you just don't care"
    p1a = "I'm so miserable and you just don't care"
    p2 = "Oh do shut up Portia"
    w1 = ["1/2/3"]
    w1a = ["1/2/3"]
    w2 = ["1/2/4"]

    JSP = JobSubmissionParameters
    jsp_sm = JSP(j1, a1, r1, u1)
    jsp_lg = JSP(j1, a1, r1, u1, p1, 1, w1)

    assert hash(jsp_sm) == hash(JSP(j1a, a1a, r1a, u1a))
    assert hash(jsp_lg) == hash(JSP(j1a, a1a, r1a, u1a, p1a, 1, w1a))

    assert hash(jsp_sm) != hash(JSP(j2, a1a, r1a, u1a))
    assert hash(jsp_sm) != hash(JSP(j1a, a2, r1a, u1a))
    assert hash(jsp_sm) != hash(JSP(j1a, a1a, r2, u1a))
    assert hash(jsp_sm) != hash(JSP(j1a, a1a, r1a, u2))

    assert hash(jsp_lg) != hash(JSP(j1a, a1a, r1a, u1a, p2, 1, w1a))
    assert hash(jsp_lg) != hash(JSP(j1a, a1a, r1a, u1a, p1a, 2, w1a))
    assert hash(jsp_lg) != hash(JSP(j1a, a1a, r1a, u1a, p1a, 1, w2))
