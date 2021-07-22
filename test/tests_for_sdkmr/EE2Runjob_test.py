"""
Unit tests for the EE2Runjob class.
"""

# Incomplete by a long way. Will add more unit tests as they come up.

import copy
from logging import Logger
from typing import List, Dict, Any
from unittest.mock import create_autospec, call

from bson.objectid import ObjectId
from pytest import raises

from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.db.models.models import Job, JobInput, JobRequirements, Meta
from execution_engine2.exceptions import (
    IncorrectParamsException,
    AuthError,
    InvalidParameterForBatch,
)
from execution_engine2.sdk.EE2Runjob import EE2RunJob, JobPermissions, JobIdPair
from execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.sdk.job_submission_parameters import (
    JobSubmissionParameters,
    JobRequirements as ResolvedRequirements,
    AppInfo,
    UserCreds,
)
from execution_engine2.utils.Condor import Condor, SubmissionInfo
from execution_engine2.utils.KafkaUtils import (
    KafkaClient,
    KafkaQueueChange,
    KafkaCreateJob,
)
from execution_engine2.utils.SlackUtils import SlackClient
from execution_engine2.utils.catalog_cache import CatalogCache
from execution_engine2.utils.job_requirements_resolver import (
    JobRequirementsResolver,
    RequirementsType,
)
from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace
from utils_shared.mock_utils import get_client_mocks, ALL_CLIENTS
from utils_shared.test_utils import assert_exception_correct

# common variables
_JOB_ID = "603051cfaf2e3401b0500982"
_GIT_COMMIT = "git5678"
_WS_REF_1 = "1/2/3"
_WS_REF_2 = "4/5/6"
_CLUSTER = "cluster42"
_METHOD = "lolcats.lol_unto_death"
_APP = "lolcats/itsmypartyilllolifiwantto"
_USER = "someuser"
_TOKEN = "tokentokentoken"
_OTHER_USER = "some_sucker"
_CREATED_STATE = "created"
_QUEUED_STATE = "queued"

# batch common variables
_BATCH = "batch"
_GIT_COMMIT_1 = "commit1"
_GIT_COMMIT_2 = "commit2"
_JOB_ID_1 = "603051cfaf2e3401b0500985"
_JOB_ID_2 = "603051cfaf2e3401b0500986"
_METHOD_1 = "module1.method1"
_APP_1 = "module1/app1"
_METHOD_2 = "module2.method2"
_APP_2 = "module2/app2"
_CLUSTER_1 = "cluster1"
_CLUSTER_2 = "cluster2"

_EMPTY_JOB_REQUIREMENTS = {
    "cpus": None,
    "memory_MB": None,
    "disk_GB": None,
    "client_group": None,
    "client_group_regex": None,
    "bill_to_user": None,
    "ignore_concurrency_limits": False,
    "scheduler_requirements": None,
    "debug_mode": None,
}


def _set_up_mocks(user: str, token: str) -> Dict[Any, Any]:
    """
    Returns a dictionary of the class that is mocked to the mock of the class, and initializes
    the SDKMR getters to return the mocks.
    """
    # Can't seem to find a mypy annotation for a class, so Any it is

    # The amount of mocking required here implies the method should be broken up into smaller
    # classes that are individually mockable. Or maybe it's just really complicated and this
    # is the best we can do. Worth looking into at some point though.
    mocks = get_client_mocks(None, None, *ALL_CLIENTS)
    sdkmr = create_autospec(SDKMethodRunner, spec_set=True, instance=True)
    mocks[SDKMethodRunner] = sdkmr
    mocks[Logger] = create_autospec(Logger, spec_set=True, instance=True)
    mocks[Workspace] = create_autospec(Workspace, spec_set=True, instance=True)
    mocks[WorkspaceAuth] = create_autospec(WorkspaceAuth, spec_set=True, instance=True)
    mocks[CatalogCache] = create_autospec(CatalogCache, spec_set=True, instance=True)

    # Set up basic getter calls
    sdkmr.get_catalog_cache.return_value = mocks[CatalogCache]
    sdkmr.get_catalog.return_value = mocks[Catalog]
    sdkmr.get_condor.return_value = mocks[Condor]
    sdkmr.get_kafka_client.return_value = mocks[KafkaClient]
    sdkmr.get_logger.return_value = mocks[Logger]
    sdkmr.get_mongo_util.return_value = mocks[MongoUtil]
    sdkmr.get_job_requirements_resolver.return_value = mocks[JobRequirementsResolver]
    sdkmr.get_slack_client.return_value = mocks[SlackClient]
    sdkmr.get_token.return_value = token
    sdkmr.get_user_id.return_value = user
    sdkmr.get_workspace.return_value = mocks[Workspace]
    sdkmr.get_workspace_auth.return_value = mocks[WorkspaceAuth]

    return mocks


def _create_job(
    reqs: ResolvedRequirements,
    user=_USER,
    method=_METHOD,
    app=_APP,
    state=_CREATED_STATE,
    git_commit=_GIT_COMMIT,
    batch_id=None,
    parent_job_id=None,
    source_ws_objects=None,
    wsid=None,
):
    job = Job()
    job.user = user
    job.status = state
    job.wsid = wsid
    job.batch_id = batch_id
    ji = JobInput()
    ji.method = method
    ji.app_id = app
    ji.wsid = wsid
    ji.service_ver = git_commit
    ji.source_ws_objects = source_ws_objects
    if parent_job_id:
        ji.parent_job_id = parent_job_id
    jr = JobRequirements()
    jr.clientgroup = reqs.client_group
    jr.cpu = reqs.cpus
    jr.memory = reqs.memory_MB
    jr.disk = reqs.disk_GB
    ji.requirements = jr
    ji.narrative_cell_info = Meta()
    job.job_input = ji
    return job


def _check_queued_job_save(got_job, job_id, cluster):
    expected_job = Job()
    expected_job.id = ObjectId(job_id)
    expected_job.status = _QUEUED_STATE
    # no way to test this really without code refactoring
    expected_job.queued = got_job.queued
    expected_job.scheduler_type = "condor"
    expected_job.scheduler_id = cluster
    assert_jobs_equal(got_job, expected_job)


def _set_up_common_return_values(mocks):
    """
    Set up return values on mocks that are the same for several tests.
    """
    mocks[Workspace].get_object_info3.return_value = {
        "paths": [[_WS_REF_1], [_WS_REF_2]]
    }
    mocks[CatalogCache].lookup_git_commit_version.return_value = _GIT_COMMIT
    mocks[SDKMethodRunner].save_job.return_value = _JOB_ID
    mocks[Condor].run_job.return_value = SubmissionInfo(_CLUSTER, {}, None)
    retjob = Job()
    retjob.id = ObjectId(_JOB_ID)
    retjob.status = _CREATED_STATE
    mocks[MongoUtil].get_job.return_value = retjob


def _check_common_mock_calls(
    mocks, reqs, wsid, app=_APP, parent_job_id=None, batch=False
):
    """
    Check that mocks are called as expected when those calls are similar or the same for
    several tests.
    """
    sdkmr = mocks[SDKMethodRunner]
    kafka = mocks[KafkaClient]
    mocks[Workspace].get_object_info3.assert_called_once_with(
        {"objects": [{"ref": _WS_REF_1}, {"ref": _WS_REF_2}], "ignoreErrors": 1}
    )
    mocks[CatalogCache].lookup_git_commit_version.assert_called_once_with(
        method="lolcats.lol_unto_death", service_ver=None
    )

    # initial job data save
    expected_job = _create_job(
        reqs,
        app=app,
        wsid=wsid,
        parent_job_id=parent_job_id,
        source_ws_objects=[_WS_REF_1, _WS_REF_2],
    )
    assert len(sdkmr.save_job.call_args_list) == 2
    got_job = sdkmr.save_job.call_args_list[0][0][0]
    assert_jobs_equal(got_job, expected_job)

    kafka.send_kafka_message.assert_any_call(KafkaCreateJob(_USER, _JOB_ID))
    jsp_expected = JobSubmissionParameters(
        _JOB_ID,
        AppInfo(_METHOD, app),
        reqs,
        UserCreds(_USER, _TOKEN),
        wsid=wsid,
        parent_job_id=parent_job_id,
        source_ws_objects=[_WS_REF_1, _WS_REF_2],
    )
    mocks[Condor].run_job.assert_called_once_with(params=jsp_expected)

    # updated job data save
    mocks[MongoUtil].get_job.assert_called_once_with(_JOB_ID)

    # update to queued state
    got_job = sdkmr.save_job.call_args_list[1][0][0]
    _check_queued_job_save(got_job, _JOB_ID, _CLUSTER)

    kafka.send_kafka_message.assert_called_with(  # update to queued state
        KafkaQueueChange(
            job_id=_JOB_ID,
            new_status=_QUEUED_STATE,
            previous_status=_CREATED_STATE,
            scheduler_id=_CLUSTER,
        )
    )
    # Removed for now, but might be added back in at a later point
    # mocks[SlackClient].run_job_message.assert_called_once_with(_JOB_ID, _CLUSTER, _USER)


def _create_reqs_dict(
    cpu,
    mem,
    disk,
    clientgroup,
    client_group_regex=None,
    ignore_concurrency_limits=None,
    debug_mode=None,
    merge_with=None,
    internal_representation=False,
):
    # the bill to user and scheduler requirements keys are different for the concierge endpoint
    # so we don't include them. If needed use the merge_with parameter.
    if internal_representation:
        ret = {
            "cpus": cpu,
            "memory_MB": mem,
            "disk_GB": disk,
        }
    else:
        ret = {
            "request_cpus": cpu,
            "request_memory": mem,
            "request_disk": disk,
        }
    ret.update(
        {
            "client_group": clientgroup,
            "client_group_regex": client_group_regex,
            "ignore_concurrency_limits": ignore_concurrency_limits,
            "debug_mode": debug_mode,
        }
    )
    if merge_with:
        ret.update(merge_with)
    return ret


def test_run_job():
    """
    A basic unit test of the run() method.

    This test is a fairly minimal test of the run() method. It does not exercise all the
    potential code paths or provide all the possible run inputs, such as job parameters, cell
    metadata, etc.
    """

    # Set up data variables
    client_group = "myfirstclientgroup"
    cpus = 1
    mem = 1
    disk = 1

    # set up mocks
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    jrr = mocks[JobRequirementsResolver]
    # We intentionally do not check the logger methods as there are a lot of them and this is
    # already a very large test. This may be something to be added later when needed.

    # Set up call returns. These calls are in the order they occur in the code
    jrr.normalize_job_reqs.return_value = {}
    jrr.get_requirements_type.return_value = RequirementsType.STANDARD
    reqs = ResolvedRequirements(
        cpus=cpus, memory_MB=mem, disk_GB=disk, client_group=client_group
    )
    jrr.resolve_requirements.return_value = reqs
    _set_up_common_return_values(mocks)

    # set up the class to be tested and run the method
    rj = EE2RunJob(sdkmr)
    params = {
        "method": _METHOD,
        "app_id": _APP,
        "source_ws_objects": [_WS_REF_1, _WS_REF_2],
    }
    assert rj.run(params) == _JOB_ID

    # check mocks called as expected. The order here is the order that they're called in the code.
    jrr.normalize_job_reqs.assert_called_once_with({}, "input job")
    jrr.get_requirements_type.assert_called_once_with(**_EMPTY_JOB_REQUIREMENTS)
    jrr.resolve_requirements.assert_called_once_with(
        _METHOD, mocks[CatalogCache], **_EMPTY_JOB_REQUIREMENTS
    )
    _check_common_mock_calls(mocks, reqs, None, _APP)


def test_run_job_as_admin_with_job_requirements_and_parent_job():
    """
    A basic unit test of the run() method with an administrative user and job requirements.

    This test is a fairly minimal test of the run() method. It does not exercise all the
    potential code paths or provide all the possible run inputs, such as job parameters, cell
    metadata, etc.

    Does not include an app_id.

    Does include a parent job id.
    """

    # Set up data variables
    client_group = "grotesquememlong"
    cpus = 4
    mem = 32
    disk = 2600

    # set up mocks
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    jrr = mocks[JobRequirementsResolver]
    # We intentionally do not check the logger methods as there are a lot of them and this is
    # already a very large test. This may be something to be added later when needed.

    # Set up call returns. These calls are in the order they occur in the code
    jrr.normalize_job_reqs.return_value = _create_reqs_dict(
        cpus, mem, disk, client_group, client_group_regex=True, debug_mode=True
    )
    jrr.get_requirements_type.return_value = RequirementsType.BILLING
    req_args = _create_reqs_dict(
        cpus,
        mem,
        disk,
        client_group,
        client_group_regex=True,
        ignore_concurrency_limits=True,
        debug_mode=True,
        merge_with={
            "bill_to_user": _OTHER_USER,
            "scheduler_requirements": {"foo": "bar", "baz": "bat"},
        },
        internal_representation=True,
    )
    reqs = ResolvedRequirements(**req_args)
    jrr.resolve_requirements.return_value = reqs
    _set_up_common_return_values(mocks)

    # set up the class to be tested and run the method
    rj = EE2RunJob(sdkmr)
    inc_reqs = _create_reqs_dict(
        cpus,
        mem,
        disk,
        client_group,
        client_group_regex=1,
        ignore_concurrency_limits="righty ho, luv",
        debug_mode="true",
        merge_with={
            "bill_to_user": _OTHER_USER,
            "scheduler_requirements": {"foo": "bar", "baz": "bat"},
        },
    )
    params = {
        "method": _METHOD,
        "source_ws_objects": [_WS_REF_1, _WS_REF_2],
        "job_requirements": inc_reqs,
        "parent_job_id": "thisislikesoooofake",
    }
    assert rj.run(params, as_admin=True) == _JOB_ID

    # check mocks called as expected. The order here is the order that they're called in the code.
    sdkmr.check_as_admin.assert_called_once_with(JobPermissions.WRITE)
    jrr.normalize_job_reqs.assert_called_once_with(inc_reqs, "input job")
    jrr.get_requirements_type.assert_called_once_with(**req_args)
    jrr.resolve_requirements.assert_called_once_with(
        _METHOD, mocks[CatalogCache], **req_args
    )
    _check_common_mock_calls(
        mocks, reqs, None, None, parent_job_id="thisislikesoooofake"
    )


def test_run_job_as_concierge_with_wsid():
    """
    A unit test of the run() method with a concierge - but not admin - user.

    Includes an app ID.
    """

    # Set up data variables
    client_group = "tinymem"
    cpus = 4
    mem = 32
    disk = 2600
    wsid = 78

    # set up mocks
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    jrr = mocks[JobRequirementsResolver]
    wsauth = mocks[WorkspaceAuth]
    # We intentionally do not check the logger methods as there are a lot of them and this is
    # already a very large test. This may be something to be added later when needed.

    # Set up call returns. These calls are in the order they occur in the code
    wsauth.can_write.return_value = True
    jrr.normalize_job_reqs.return_value = _create_reqs_dict(
        cpus, mem, disk, client_group, client_group_regex=False, debug_mode=True
    )
    reqs = ResolvedRequirements(
        cpus=cpus,
        memory_MB=mem,
        disk_GB=disk,
        client_group=client_group,
        client_group_regex=False,
        ignore_concurrency_limits=False,
        bill_to_user=_OTHER_USER,
        scheduler_requirements={"foo": "bar", "baz": "bat"},
        debug_mode=True,
    )
    jrr.resolve_requirements.return_value = reqs
    _set_up_common_return_values(mocks)

    # set up the class to be tested and run the method
    rj = EE2RunJob(sdkmr)
    params = {
        "method": _METHOD,
        "app_id": _APP,
        "wsid": wsid,
        "source_ws_objects": [_WS_REF_1, _WS_REF_2],
    }
    conc_params = _create_reqs_dict(
        cpus,
        mem,
        disk,
        client_group,
        client_group_regex=0,
        ignore_concurrency_limits=0,
        debug_mode=1,
        merge_with={
            "account_group": _OTHER_USER,
            "requirements_list": ["  foo   =   bar   ", "baz=bat"],
        },
    )
    assert rj.run(params, concierge_params=conc_params) == _JOB_ID

    # check mocks called as expected. The order here is the order that they're called in the code.
    sdkmr.check_as_concierge.assert_called_once_with()
    wsauth.can_write.assert_called_once_with(wsid)
    jrr.normalize_job_reqs.assert_called_once_with(conc_params, "concierge parameters")

    jrr.resolve_requirements.assert_called_once_with(
        _METHOD,
        mocks[CatalogCache],
        cpus=cpus,
        memory_MB=mem,
        disk_GB=disk,
        client_group=client_group,
        client_group_regex=False,
        ignore_concurrency_limits=False,
        bill_to_user=_OTHER_USER,
        scheduler_requirements={"foo": "bar", "baz": "bat"},
        debug_mode=True,
    )
    _check_common_mock_calls(mocks, reqs, wsid)


def test_run_job_as_concierge_empty_as_admin():
    """
    A unit test of the run() method with an effectively empty concierge dict and admin privs.
    The fake key should be ignored but is required to make the concierge params truthy and
    trigger the pathway.

    Also provides a module only app ID, as some KBase processes provide these.
    """
    _run_as_concierge_empty_as_admin({"fake": "foo"}, "lolcats")


def test_run_job_as_concierge_sched_reqs_None_as_admin():
    """
    A unit test of the run() method with an concierge dict containing None for the scheduler
    requirements and admin privs.

    Also provides an app ID with a . instead of a /
    """
    _run_as_concierge_empty_as_admin(
        {"requirements_list": None}, "lolcats.itsmypartyilllolifiwantto"
    )


def test_run_job_as_concierge_sched_reqs_empty_list_as_admin():
    """
    A unit test of the run() method with an concierge dict containing an empty list for the
    scheduler requirements and admin privs.
    """
    _run_as_concierge_empty_as_admin({"requirements_list": []}, _APP)


def _run_as_concierge_empty_as_admin(concierge_params, app):
    # Set up data variables
    client_group = "concierge"  # hardcoded default for run_as_concierge
    cpus = 1
    mem = 1
    disk = 1

    # set up mocks
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    jrr = mocks[JobRequirementsResolver]
    # We intentionally do not check the logger methods as there are a lot of them and this is
    # already a very large test. This may be something to be added later when needed.

    # Set up call returns. These calls are in the order they occur in the code
    jrr.normalize_job_reqs.return_value = {}
    reqs = ResolvedRequirements(
        cpus=cpus,
        memory_MB=mem,
        disk_GB=disk,
        client_group=client_group,
    )
    jrr.resolve_requirements.return_value = reqs
    _set_up_common_return_values(mocks)

    # set up the class to be tested and run the method
    rj = EE2RunJob(sdkmr)
    params = {
        "method": _METHOD,
        "app_id": app,
        "source_ws_objects": [_WS_REF_1, _WS_REF_2],
    }
    assert rj.run(params, concierge_params=concierge_params, as_admin=True) == _JOB_ID

    # check mocks called as expected. The order here is the order that they're called in the code.
    sdkmr.check_as_admin.assert_called_once_with(JobPermissions.WRITE)
    sdkmr.check_as_concierge.assert_called_once_with()
    jrr.normalize_job_reqs.assert_called_once_with(
        concierge_params, "concierge parameters"
    )

    jrr.resolve_requirements.assert_called_once_with(
        _METHOD,
        mocks[CatalogCache],
        cpus=None,
        memory_MB=None,
        disk_GB=None,
        client_group=client_group,
        client_group_regex=None,
        ignore_concurrency_limits=True,
        bill_to_user=None,
        scheduler_requirements={},
        debug_mode=None,
    )
    _check_common_mock_calls(mocks, reqs, None, app)


def test_run_job_concierge_fail_bad_params():
    """
    Test that submitting invalid concierge params causes the job to fail. Note that most
    error checking happens in the mocked out job requirements resolver, so we only check for
    errors that EE2RunJob is responsible for handling.
    """
    _run_fail_concierge_params(
        {"requirements_list": {"a", "b"}},
        IncorrectParamsException("requirements_list must be a list"),
    )
    for err in [None, "", 42, "foo:bar"]:
        _run_fail_concierge_params(
            {"requirements_list": [err]},
            IncorrectParamsException(
                f"Found illegal requirement in requirements_list: {err}"
            ),
        )


def _run_fail_concierge_params(concierge_params, expected):
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    jrr = mocks[JobRequirementsResolver]
    jrr.normalize_job_reqs.return_value = {}

    rj = EE2RunJob(sdkmr)
    params = {
        "method": _METHOD,
        "app_id": _APP,
    }
    with raises(Exception) as got:
        rj.run(params, concierge_params=concierge_params)
    assert_exception_correct(got.value, expected)


def test_run_job_and_run_job_batch_fail_illegal_arguments():
    """
    Test that illegal arguments cause the job to fail. Note that not all arguments are
    checked - this test checks arguments that are checked in the _check_job_arguments()
    method. Furthermore, most argument checking occurs in the job submission parameters
    class and its respective composed classes, and we don't reproduce all the error conditions
    possible - just enough to ensure the error checking occurs. If major changes are made to
    the error checking code then more tests may need to be written.

    Tests both the run() and run_batch() methods.
    """
    # These are extremely annoying to debug as they don't raise a stacktrace if a different exception type was thrown
    # or let you know that it was an entirely different exception, or if the exception happened in the bulk version of the run

    _run_and_run_batch_fail_illegal_arguments(
        {}, IncorrectParamsException("Missing input parameter: method ID")
    )

    _run_and_run_batch_fail_illegal_arguments(
        {"method": "foo.bar", "wsid": 0},
        IncorrectParamsException("wsid must be at least 1"),
        InvalidParameterForBatch(),
    )
    _run_and_run_batch_fail_illegal_arguments(
        {"method": "foo.bar", "wsid": -1},
        IncorrectParamsException("wsid must be at least 1"),
        InvalidParameterForBatch(),
    )
    _run_and_run_batch_fail_illegal_arguments(
        {"method": "foo.bar", "source_ws_objects": {"a": "b"}},
        IncorrectParamsException("source_ws_objects must be a list"),
    )
    _run_and_run_batch_fail_illegal_arguments(
        {"method": "foo.bar", "job_requirements": ["10 bob", "a pickled egg"]},
        IncorrectParamsException("job_requirements must be a mapping"),
    )
    _run_and_run_batch_fail_illegal_arguments(
        {
            "method": "foo.bar",
            "job_requirements": {
                "bill_to_user": {
                    "Bill": "$3.78",
                    "Boris": "$2.95",
                    "AJ": "one BILIIOOOON dollars",
                    "Sumin": "$1,469,890.42",
                }
            },
        },
        IncorrectParamsException("bill_to_user must be a string"),
    )


def _run_and_run_batch_fail_illegal_arguments(params, expected, batch_expected=None):
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    jrr.resolve_requirements.return_value = ResolvedRequirements(1, 1, 1, "cg")
    _run_and_run_batch_fail(mocks[SDKMethodRunner], params, expected, batch_expected)


def test_run_job_and_run_job_batch_fail_arg_normalization():
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    e = "Found illegal request_cpus 'like 10 I guess? IDK' in job requirements from input job"
    jrr.normalize_job_reqs.side_effect = IncorrectParamsException(e)
    _run_and_run_batch_fail(
        mocks[SDKMethodRunner],
        {
            "method": "foo.bar",
            "job_requirements": {"request_cpus": "like 10 I guess? IDK"},
        },
        IncorrectParamsException(e),
    )


def test_run_job_and_run_job_batch_fail_get_requirements_type():
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    jrr.normalize_job_reqs.return_value = {}
    e = "bill_to_user contains control characters"
    jrr.get_requirements_type.side_effect = IncorrectParamsException(e)
    _run_and_run_batch_fail(
        mocks[SDKMethodRunner],
        {"method": "foo.bar", "job_requirements": {"bill_to_user": "ding\bding"}},
        IncorrectParamsException(e),
    )


def test_run_job_and_run_job_batch_fail_not_admin_with_job_reqs():
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    jrr.normalize_job_reqs.return_value = {}
    jrr.get_requirements_type.return_value = RequirementsType.PROCESSING
    _run_and_run_batch_fail(
        mocks[SDKMethodRunner],
        {"method": "foo.bar", "job_requirements": {"ignore_concurrency_limits": 1}},
        AuthError("In order to specify job requirements you must be a full admin"),
        as_admin=False,
    )


def test_run_job_and_run_job_batch_fail_resolve_requirements():
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    jrr.normalize_job_reqs.return_value = {}
    jrr.get_requirements_type.return_value = RequirementsType.STANDARD
    e = "Unrecognized method: 'None'. Please input module_name.function_name"
    jrr.resolve_requirements.side_effect = IncorrectParamsException(e)
    _run_and_run_batch_fail(mocks[SDKMethodRunner], {}, IncorrectParamsException(e))


def test_run_job_and_run_job_batch_fail_workspace_objects_check():
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    jrr = mocks[JobRequirementsResolver]
    jrr.resolve_requirements.return_value = ResolvedRequirements(1, 1, 1, "cg")
    mocks[Workspace].get_object_info3.return_value = {
        "paths": ["1/2/3", None, "21/34/55"]
    }

    params = {
        "method": "foo.bar",
        "app_id": "foo/baz",
        "source_ws_objects": ["1/2/3", "5/8/13", "21/34/55"],
    }
    _run_and_run_batch_fail(
        sdkmr, params, ValueError("Some workspace object is inaccessible")
    )


def _run_and_run_batch_fail(
    sdkmr, params, expected, batch_expected=None, as_admin=True
):
    rj = EE2RunJob(sdkmr)
    with raises(Exception) as got:
        rj.run(params, as_admin=as_admin)
    assert_exception_correct(got.value, expected)

    if batch_expected:
        expected = batch_expected
    _run_batch_fail(rj, [params], {}, as_admin, expected)


def _set_up_common_return_values_batch(mocks):
    """
    Set up return values on mocks that are the same for several tests.
    """
    mocks[Workspace].get_object_info3.return_value = {
        "paths": [[_WS_REF_1], [_WS_REF_2]]
    }
    returned_parent_job = Job()
    returned_parent_job.id = ObjectId(_JOB_ID)
    returned_parent_job.user = _USER

    mocks[SDKMethodRunner].save_and_return_job.return_value = returned_parent_job
    mocks[CatalogCache].lookup_git_commit_version.side_effect = [
        _GIT_COMMIT_1,
        _GIT_COMMIT_2,
    ]

    # create job1, update job1, create job2, update job2, update parent job
    mocks[SDKMethodRunner].save_job.side_effect = [
        _JOB_ID_1,
        None,
        _JOB_ID_2,
        None,
        None,
    ]

    mocks[SDKMethodRunner].save_jobs.side_effect = [
        [_JOB_ID_1, _JOB_ID_2],
    ]

    mocks[Condor].run_job.side_effect = [
        SubmissionInfo(_CLUSTER_1, {}, None),
        SubmissionInfo(_CLUSTER_2, {}, None),
    ]
    retjob_1 = Job()
    retjob_1.id = ObjectId(_JOB_ID_1)
    retjob_1.status = _CREATED_STATE
    retjob_2 = Job()
    retjob_2.id = ObjectId(_JOB_ID_2)
    retjob_2.status = _CREATED_STATE
    mocks[MongoUtil].get_job.side_effect = [retjob_1, retjob_2]


def _check_common_mock_calls_batch(mocks, reqs1, reqs2, parent_wsid):
    """
    Check that mocks are called as expected when those calls are similar or the same for
    several tests.
    """
    sdkmr = mocks[SDKMethodRunner]
    mocks[Workspace].get_object_info3.assert_called_once_with(
        {"objects": [{"ref": _WS_REF_1}, {"ref": _WS_REF_2}], "ignoreErrors": 1}
    )

    # parent job initial save
    expected_parent_job = Job()
    job_input = JobInput()
    job_input.service_ver = _BATCH
    job_input.app_id = _BATCH
    job_input.method = _BATCH
    job_input.narrative_cell_info = Meta()
    expected_parent_job.job_input = job_input
    expected_parent_job.batch_job = True
    expected_parent_job.status = _CREATED_STATE
    expected_parent_job.wsid = parent_wsid
    expected_parent_job.user = _USER
    assert len(sdkmr.save_and_return_job.call_args_list) == 1
    got_parent_job = sdkmr.save_and_return_job.call_args_list[0][0][0]
    assert_jobs_equal(got_parent_job, expected_parent_job)

    mocks[CatalogCache].lookup_git_commit_version.assert_has_calls(
        [
            call(method="module1.method1", service_ver=None),
            call(method="module2.method2", service_ver=None),
        ]
    )

    assert len(sdkmr.save_jobs.call_args_list) == 1

    # initial child jobs data save
    expected_job_1 = _create_job(
        reqs1,
        method=_METHOD_1,
        app=_APP_1,
        git_commit=_GIT_COMMIT_1,
        source_ws_objects=[_WS_REF_1, _WS_REF_2],
        wsid=parent_wsid,
        batch_id=_JOB_ID,
    )
    got_job_1 = sdkmr.save_jobs.call_args_list[0][0][0][0]
    assert_jobs_equal(got_job_1, expected_job_1)

    expected_job_2 = _create_job(
        reqs2,
        method=_METHOD_2,
        app=_APP_2,
        git_commit=_GIT_COMMIT_2,
        wsid=parent_wsid,
        batch_id=_JOB_ID,
    )
    # index 1 because save_jobs returns a list of two jobs
    got_job_2 = sdkmr.save_jobs.call_args_list[0][0][0][1]
    assert_jobs_equal(got_job_2, expected_job_2)

    jsp_expected_1 = JobSubmissionParameters(
        _JOB_ID_1,
        AppInfo(_METHOD_1, _APP_1),
        reqs1,
        UserCreds(_USER, _TOKEN),
        parent_job_id=_JOB_ID,
        source_ws_objects=[_WS_REF_1, _WS_REF_2],
        wsid=parent_wsid,
    )
    jsp_expected_2 = JobSubmissionParameters(
        _JOB_ID_2,
        AppInfo(_METHOD_2, _APP_2),
        reqs2,
        UserCreds(_USER, _TOKEN),
        parent_job_id=_JOB_ID,
        wsid=parent_wsid,
    )
    mocks[Condor].run_job.assert_has_calls(
        [call(params=jsp_expected_1), call(params=jsp_expected_2)]
    )

    # update to queued state
    child_job_pairs = [
        JobIdPair(_JOB_ID_1, _CLUSTER_1),
        JobIdPair(_JOB_ID_2, _CLUSTER_2),
    ]
    mocks[MongoUtil].update_jobs_to_queued.assert_has_calls([call(child_job_pairs)])

    mocks[KafkaClient].send_kafka_message.assert_has_calls(
        [
            call(KafkaCreateJob(job_id=_JOB_ID, user=_USER)),  # parent job
            call(KafkaCreateJob(job_id=_JOB_ID_1, user=_USER)),
            call(KafkaCreateJob(job_id=_JOB_ID_2, user=_USER)),
            call(
                KafkaQueueChange(
                    job_id=_JOB_ID_1,
                    new_status=_QUEUED_STATE,
                    previous_status=_CREATED_STATE,
                    scheduler_id=_CLUSTER_1,
                )
            ),
            call(
                KafkaQueueChange(
                    job_id=_JOB_ID_2,
                    new_status=_QUEUED_STATE,
                    previous_status=_CREATED_STATE,
                    scheduler_id=_CLUSTER_2,
                )
            ),
        ]
    )

    # Removed for now, but might be added back in if run_job_message is re-added
    # mocks[SlackClient].run_job_message.assert_has_calls(
    #     [
    #         call(job_id=_JOB_ID_1, scheduler_id=_CLUSTER_1, username=_USER),
    #         call(job_id=_JOB_ID_2, scheduler_id=_CLUSTER_2, username=_USER),
    #     ]
    # )

    # Test to see if add_child jobs is called with correct batch_container and children
    expected_batch_container = Job()
    expected_batch_container.id = ObjectId(_JOB_ID)
    expected_batch_container.user = _USER

    batch_job = sdkmr.add_child_jobs.call_args_list[0][1]["batch_job"]
    sdkmr.add_child_jobs.assert_called_once_with(
        batch_job=expected_batch_container, child_jobs=[_JOB_ID_1, _JOB_ID_2]
    )
    """
    So this test doesn't actually check that the call is correct, but the assert_jobs_equal line below does
    the assert below is necessary because of how equality works for Job objects
    (  print(jobs_to_update) because they have the same object ID, which is what Job equality is based on. )
    and that the assert_called_once_with doesn't correctly check the job object
    """
    assert_jobs_equal(batch_job, expected_batch_container)


def test_run_job_batch_with_parent_job_wsid():
    """
    A basic unit test of the run_batch() method, providing a workspace ID for the parent job.

    This test is a fairly minimal test of the run_batch() method. It does not exercise all the
    potential code paths or provide all the possible run inputs, such as job parameters, cell
    metadata, etc.
    """
    # When an assertion is failed, this test doesn't show you where failed in PyCharm, so use
    # Additional arguments `--no-cov -s` or run from cmd line
    # PYTHONPATH=.:lib:test pytest test/tests_for_sdkmr/EE2Runjob_test.py::test_run_job_batch_with_parent_job_wsid --no-cov

    # set up variables
    parent_wsid = 89
    wsid = 32

    # set up mocks
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    jrr = mocks[JobRequirementsResolver]
    # We intentionally do not check the logger methods as there are a lot of them and this is
    # already a very large test. This may be something to be added later when needed.

    # Set up call returns. These calls are in the order they occur in the code
    mocks[WorkspaceAuth].can_write.return_value = True
    mocks[WorkspaceAuth].can_write_list.return_value = {wsid: True}

    jrr.normalize_job_reqs.side_effect = [{}, {}]
    jrr.get_requirements_type.side_effect = [
        RequirementsType.STANDARD,
        RequirementsType.STANDARD,
    ]
    reqs1 = ResolvedRequirements(
        cpus=1,
        memory_MB=2,
        disk_GB=3,
        client_group="cg1",
    )
    reqs2 = ResolvedRequirements(
        cpus=10,
        memory_MB=20,
        disk_GB=30,
        client_group="cg2",
    )
    jrr.resolve_requirements.side_effect = [reqs1, reqs2]

    _set_up_common_return_values_batch(mocks)

    # set up the class to be tested and run the method
    rj = EE2RunJob(sdkmr)
    params = [
        {
            "method": _METHOD_1,
            "app_id": _APP_1,
            "source_ws_objects": [_WS_REF_1, _WS_REF_2],
        },
        {
            "method": _METHOD_2,
            "app_id": _APP_2,
            "wsid": wsid,
        },
    ]
    with raises(InvalidParameterForBatch) as got:
        rj.run_batch(copy.deepcopy(params), {"wsid": parent_wsid})
    assert_exception_correct(got.value, InvalidParameterForBatch())

    params[1]["wsid"] = None
    assert rj.run_batch(params, {"wsid": parent_wsid}) == {
        "batch_id": _JOB_ID,
        "child_job_ids": [_JOB_ID_1, _JOB_ID_2],
    }

    # check mocks called as expected. The order here is the order that they're called in the code.
    mocks[WorkspaceAuth].can_write.assert_called_once_with(parent_wsid)

    jrr = mocks[JobRequirementsResolver]
    jrr.normalize_job_reqs.assert_has_calls(
        [call({}, "input job"), call({}, "input job")]
    )
    jrr.get_requirements_type.assert_has_calls(
        [call(**_EMPTY_JOB_REQUIREMENTS), call(**_EMPTY_JOB_REQUIREMENTS)]
    )
    jrr.resolve_requirements.assert_has_calls(
        [
            call(_METHOD_1, mocks[CatalogCache], **_EMPTY_JOB_REQUIREMENTS),
            call(_METHOD_2, mocks[CatalogCache], **_EMPTY_JOB_REQUIREMENTS),
        ]
    )
    _check_common_mock_calls_batch(mocks, reqs1, reqs2, parent_wsid)


def test_run_job_batch_as_admin_with_job_requirements():
    """
    A basic unit test of the run_batch() method with an administrative user and supplied job
    requirements.

    This test is a fairly minimal test of the run_batch() method. It does not exercise all the
    potential code paths or provide all the possible run inputs, such as job parameters, cell
    metadata, etc.
    """
    # set up variables
    cpus = 89
    mem = 3
    disk = 10000
    client_group = "verylargeclientgroup"

    # set up mocks
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    jrr = mocks[JobRequirementsResolver]
    # We intentionally do not check the logger methods as there are a lot of them and this is
    # already a very large test. This may be something to be added later when needed.

    # Set up call returns. These calls are in the order they occur in the code
    jrr.normalize_job_reqs.side_effect = [
        {},
        _create_reqs_dict(
            cpus, mem, disk, client_group, client_group_regex=True, debug_mode=True
        ),
    ]
    jrr.get_requirements_type.side_effect = [
        RequirementsType.STANDARD,
        RequirementsType.BILLING,
    ]
    req_args = _create_reqs_dict(
        cpus,
        mem,
        disk,
        client_group,
        client_group_regex=True,
        ignore_concurrency_limits=True,
        debug_mode=True,
        merge_with={
            "bill_to_user": _OTHER_USER,
            "scheduler_requirements": {"foo": "bar", "baz": "bat"},
        },
        internal_representation=True,
    )
    reqs1 = ResolvedRequirements(
        cpus=1, memory_MB=1, disk_GB=1, client_group="verysmallclientgroup"
    )
    reqs2 = ResolvedRequirements(**req_args)
    jrr.resolve_requirements.side_effect = [reqs1, reqs2]

    _set_up_common_return_values_batch(mocks)

    # set up the class to be tested and run the method
    rj = EE2RunJob(sdkmr)
    inc_reqs = _create_reqs_dict(
        cpus,
        mem,
        disk,
        client_group,
        client_group_regex=1,
        ignore_concurrency_limits="righty ho, luv",
        debug_mode="true",
        merge_with={
            "bill_to_user": _OTHER_USER,
            "scheduler_requirements": {"foo": "bar", "baz": "bat"},
        },
    )
    params = [
        {
            "method": _METHOD_1,
            "app_id": _APP_1,
            "source_ws_objects": [_WS_REF_1, _WS_REF_2],
        },
        {
            "method": _METHOD_2,
            "app_id": _APP_2,
            "job_requirements": inc_reqs,
        },
    ]
    assert rj.run_batch(params, {}, as_admin=True) == {
        "batch_id": _JOB_ID,
        "child_job_ids": [_JOB_ID_1, _JOB_ID_2],
    }

    # check mocks called as expected. The order here is the order that they're called in the code.
    sdkmr.check_as_admin.assert_called_once_with(JobPermissions.WRITE)
    jrr.normalize_job_reqs.assert_has_calls(
        [call({}, "input job"), call(inc_reqs, "input job")]
    )
    jrr.get_requirements_type.assert_has_calls(
        [call(**_EMPTY_JOB_REQUIREMENTS), call(**req_args)]
    )
    jrr.resolve_requirements.assert_has_calls(
        [
            call(_METHOD_1, mocks[CatalogCache], **_EMPTY_JOB_REQUIREMENTS),
            call(_METHOD_2, mocks[CatalogCache], **req_args),
        ]
    )
    _check_common_mock_calls_batch(mocks, reqs1, reqs2, None)


def test_run_batch_preflight_failures():
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    rj = EE2RunJob(sdkmr)
    with raises(Exception) as got:
        rj._preflight(runjob_params=[], batch_params=[])

    assert_exception_correct(
        got.value,
        expected=IncorrectParamsException(
            "RunJobParams and BatchParams cannot be identical"
        ),
    )

    with raises(Exception) as got:
        rj._preflight(runjob_params=[], batch_params={"batch": "batch"})

    assert_exception_correct(
        got.value,
        expected=IncorrectParamsException(
            "Programming error, you forgot to set the new_batch_job flag to True"
        ),
    )


def test_run_batch_fail_params_not_list_or_batch_not_mapping():
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    rj = EE2RunJob(sdkmr)
    for params in [
        None,
        {},
        {
            1,
        },
        "a",
        8,
    ]:
        _run_batch_fail(
            rj, params, {}, True, IncorrectParamsException("params must be a list")
        )

    _run_batch_fail(
        rj, [], [], True, IncorrectParamsException("batch params must be a mapping")
    )


# Note the next few tests are specifically testing that errors for multiple jobs have the
# correct job number


def test_run_job_batch_fail_illegal_arguments():
    """
    Test that illegal arguments cause the job to fail. Note that not all arguments are
    checked - this test checks arguments that are checked in the _check_job_arguments()
    method. Furthermore, most argument checking occurs in the job submission parameters
    class and its respective composed classes, and we don't reproduce all the error conditions
    possible - just enough to ensure the error checking occurs. If major changes are made to
    the error checking code then more tests may need to be written.

    """
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    jrr.resolve_requirements.return_value = ResolvedRequirements(1, 1, 1, "cg")
    rj = EE2RunJob(mocks[SDKMethodRunner])
    job = {"method": "foo.bar"}

    _run_batch_fail(
        rj,
        [job, job, {}],
        {},
        True,
        IncorrectParamsException("Job #3: Missing input parameter: method ID"),
    )
    _run_batch_fail(
        rj,
        [job, {"method": "foo.bar", "wsid": 0}],
        {},
        True,
        InvalidParameterForBatch(),
    )
    _run_batch_fail(
        rj,
        [{"method": "foo.bar", "source_ws_objects": {"a": "b"}}, job],
        {},
        True,
        IncorrectParamsException("Job #1: source_ws_objects must be a list"),
    )
    _run_batch_fail(
        rj,
        [job, {"method": "foo.bar", "job_requirements": ["10 bob", "a pickled egg"]}],
        {},
        True,
        IncorrectParamsException("Job #2: job_requirements must be a mapping"),
    )
    _run_batch_fail(
        rj,
        [{"method": "foo.bar", "job_requirements": {"bill_to_user": 1}}, job],
        {},
        True,
        IncorrectParamsException("Job #1: bill_to_user must be a string"),
    )


def test_run_job_batch_fail_arg_normalization():
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    e = "Found illegal request_cpus 'like 10 I guess? IDK' in job requirements from input job"
    jrr.normalize_job_reqs.side_effect = [{}, IncorrectParamsException(e)]
    _run_batch_fail(
        EE2RunJob(mocks[SDKMethodRunner]),
        [
            {"method": "foo.bar"},
            {
                "method": "foo.bar",
                "job_requirements": {"request_cpus": "like 10 I guess? IDK"},
            },
        ],
        {},
        True,
        IncorrectParamsException("Job #2: " + e),
    )


def test_run_job_batch_fail_get_requirements_type():
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    jrr.normalize_job_reqs.return_value = {}
    e = "bill_to_user contains control characters"
    jrr.get_requirements_type.side_effect = [
        RequirementsType.STANDARD,
        RequirementsType.STANDARD,
        IncorrectParamsException(e),
    ]
    _run_batch_fail(
        EE2RunJob(mocks[SDKMethodRunner]),
        [
            {"method": "foo.bar"},
            {"method": "foo.bar"},
            {"method": "foo.bar", "job_requirements": {"bill_to_user": "ding\bding"}},
        ],
        {},
        False,
        IncorrectParamsException("Job #3: " + e),
    )


def test_run_job_batch_fail_not_admin_with_job_reqs():
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    jrr.normalize_job_reqs.return_value = {}
    jrr.get_requirements_type.side_effect = [
        RequirementsType.PROCESSING,
        RequirementsType.STANDARD,
    ]
    _run_batch_fail(
        EE2RunJob(mocks[SDKMethodRunner]),
        [
            {"method": "foo.bar", "job_requirements": {"ignore_concurrency_limits": 1}},
            {"method": "foo.bar"},
        ],
        {},
        False,
        AuthError(
            "Job #1: In order to specify job requirements you must be a full admin"
        ),
    )


def test_run_job_batch_fail_resolve_requirements():
    mocks = _set_up_mocks(_USER, _TOKEN)
    jrr = mocks[JobRequirementsResolver]
    jrr.normalize_job_reqs.return_value = {}
    jrr.get_requirements_type.return_value = RequirementsType.STANDARD
    e = "Unrecognized method: 'None'. Please input module_name.function_name"
    jr = ResolvedRequirements(cpus=4, memory_MB=4, disk_GB=4, client_group="cg")
    jrr.resolve_requirements.side_effect = [jr, IncorrectParamsException(e)]
    _run_batch_fail(
        EE2RunJob(mocks[SDKMethodRunner]),
        [{}, {"method": "foo.bar"}],
        {},
        False,
        IncorrectParamsException("Job #2: " + e),
    )


def test_run_job_batch_fail_parent_id_included():
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    rj = EE2RunJob(sdkmr)

    _run_batch_fail(
        rj,
        [{"method": "foo.bar", "app_id": "foo/bat", "parent_job_id": "a"}],
        {},
        True,
        IncorrectParamsException("batch jobs may not specify a parent job ID"),
    )

    _run_batch_fail(
        rj,
        [
            {"method": "foo.bar", "app_id": "foo/bat"},
            {"method": "foo.bar", "app_id": "foo/bat", "parent_job_id": "a"},
        ],
        {},
        True,
        IncorrectParamsException("Job #2: batch jobs may not specify a parent job ID"),
    )


def _run_batch_fail(run_job, params, batch_params, as_admin, expected):
    with raises(Exception) as got:
        run_job.run_batch(params, batch_params, as_admin=as_admin)
    assert_exception_correct(got.value, expected)


def assert_jobs_equal(got_job: Job, expected_job: Job):
    """
    Checks that the two jobs are equivalent, except that the 'updated' fields are checked that
    they're within 1 second of each other.
    """
    # Job inherits from Document which inherits from BaseDocument in MongoEngine. BD provides
    # the __eq__ method for the hierarchy, which bases equality on the Jobs having equal id
    # fields, or if no id is present, on identity. Therefore
    # assert job1 == job2
    # will not work as a test mechanic.
    # JobInput and its contained classes inherit from EmbeddedDocument which *does* have an
    # __eq__ method that takes the class fields into account.
    # Also note that all these classes use __slots__ so vars() and __dict__ are empty other
    # than the class name.
    # Hence we do this disgusting hack instead. Note it will need to be updated any time a
    # job field is added.

    if not hasattr(got_job, "id"):
        assert not hasattr(expected_job, "id")
    else:
        assert got_job.id == expected_job.id

    # The Job class fills the updated field with the output of time.time on instantiation
    # so we can't do a straight equality
    assert abs(got_job.updated - expected_job.updated) < 1

    job_fields = [
        "user",
        "authstrat",
        "wsid",
        "status",
        "queued",
        "estimating",
        "running",
        "finished",
        "errormsg",
        "msg",
        "error",
        "terminated_code",
        "error_code",
        "scheduler_type",
        "scheduler_id",
        "scheduler_estimator_id",
        "job_input",
        "job_output",
        "condor_job_ads",
        "child_jobs",
        "batch_job",
        "batch_id",
    ]

    _assert_field_subset_equal(got_job, expected_job, job_fields)


def _assert_field_subset_equal(obj1: object, obj2: object, fields: List[str]):
    """
    Checks that field subsets from two objects are the same.

    :param obj1: The first object
    :param obj2: The second object
    :param fields: The fields in the objects to compare for equality. Any fields in the object
        not in this list are ignored and not included in the equality calculation.
    :raises AttributeError: If the field is not present in one or both of the objects.
    """
    for field in fields:
        assert getattr(obj1, field) == getattr(obj2, field), field
