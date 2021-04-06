"""
Unit tests for the EE2Runjob class.
"""

# Incomplete by a long way. Will add more unit tests as they come up.

from typing import List, Dict, Any
from bson.objectid import ObjectId
from logging import Logger
from unittest.mock import create_autospec
from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.db.models.models import Job, JobInput, JobRequirements, Meta
from execution_engine2.sdk.EE2Runjob import EE2RunJob, JobPermissions
from execution_engine2.utils.CatalogUtils import CatalogUtils
from execution_engine2.utils.Condor import Condor, SubmissionInfo, CondorResources
from execution_engine2.sdk.job_submission_parameters import (
    JobRequirements as ResolvedRequirements,
)
from execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.utils.KafkaUtils import (
    KafkaClient,
    KafkaQueueChange,
    KafkaCreateJob,
)
from execution_engine2.utils.job_requirements_resolver import JobRequirementsResolver
from execution_engine2.utils.SlackUtils import SlackClient
from execution_engine2.db.MongoUtil import MongoUtil
from installed_clients.WorkspaceClient import Workspace
from installed_clients.CatalogClient import Catalog
from utils_shared.mock_utils import get_client_mocks, ALL_CLIENTS

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
_CREATED_STATE = "created"
_QUEUED_STATE = "queued"


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
    # Set up basic getter calls
    sdkmr.get_catalog.return_value = mocks[Catalog]
    sdkmr.get_catalog_utils.return_value = mocks[CatalogUtils]
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


def _set_up_common_return_values(mocks):
    """
    Set up return values on mocks that are the same for several tests.
    """
    mocks[Workspace].get_object_info3.return_value = {
        "paths": [[_WS_REF_1], [_WS_REF_2]]
    }
    mocks[SDKMethodRunner].save_job.return_value = _JOB_ID
    mocks[Catalog].get_module_version.return_value = {"git_commit_hash": _GIT_COMMIT}
    mocks[Condor].run_job.return_value = SubmissionInfo(_CLUSTER, {}, None)
    retjob = Job()
    retjob.id = ObjectId(_JOB_ID)
    retjob.status = _CREATED_STATE
    mocks[MongoUtil].get_job.return_value = retjob


def _check_common_mock_calls(mocks, reqs, creqs, wsid):
    """
    Check that mocks are called as expected when those calls are similar or the same for
    several tests.
    """
    sdkmr = mocks[SDKMethodRunner]
    kafka = mocks[KafkaClient]
    mocks[Workspace].get_object_info3.assert_called_once_with(
        {"objects": [{"ref": _WS_REF_1}, {"ref": _WS_REF_2}], "ignoreErrors": 1}
    )
    mocks[Catalog].get_module_version.assert_called_once_with(
        {"module_name": "lolcats", "version": "release"}
    )

    # initial job data save
    expected_job = Job()
    expected_job.user = _USER
    expected_job.status = _CREATED_STATE
    expected_job.wsid = wsid
    ji = JobInput()
    ji.method = _METHOD
    ji.app_id = _APP
    ji.wsid = wsid
    ji.service_ver = _GIT_COMMIT
    ji.source_ws_objects = [_WS_REF_1, _WS_REF_2]
    ji.parent_job_id = "None"
    jr = JobRequirements()
    jr.clientgroup = creqs.client_group
    jr.cpu = creqs.request_cpus
    jr.memory = creqs.request_memory
    jr.disk = creqs.request_disk
    ji.requirements = jr
    ji.narrative_cell_info = Meta()
    expected_job.job_input = ji
    assert len(sdkmr.save_job.call_args_list) == 2
    got_job = sdkmr.save_job.call_args_list[0][0][0]
    assert_jobs_equal(got_job, expected_job)

    kafka.send_kafka_message.assert_any_call(KafkaCreateJob(_USER, _JOB_ID))
    params_expected = {
        "method": _METHOD,
        "app_id": _APP,
        "source_ws_objects": [_WS_REF_1, _WS_REF_2],
        "service_ver": _GIT_COMMIT,
        "job_id": _JOB_ID,
        "user_id": _USER,
        "token": _TOKEN,
        "cg_resources_requirements": reqs,
    }
    mocks[Condor].run_job.assert_called_once_with(
        params=params_expected, concierge_params=None
    )

    # updated job data save
    mocks[MongoUtil].get_job.assert_called_once_with(_JOB_ID)

    # update to queued state
    got_job = sdkmr.save_job.call_args_list[1][0][0]
    expected_job = Job()
    expected_job.id = ObjectId(_JOB_ID)
    expected_job.status = _QUEUED_STATE
    # no way to test this really without code refactoring
    expected_job.queued = got_job.queued

    expected_job.scheduler_type = "condor"
    expected_job.scheduler_id = _CLUSTER
    assert_jobs_equal(got_job, expected_job)

    kafka.send_kafka_message.assert_called_with(  # update to queued state
        KafkaQueueChange(
            job_id=_JOB_ID,
            new_status=_QUEUED_STATE,
            previous_status=_CREATED_STATE,
            scheduler_id=_CLUSTER,
        )
    )
    mocks[SlackClient].run_job_message.assert_called_once_with(_JOB_ID, _CLUSTER, _USER)


def test_run_as_admin():
    """
    A basic unit test of the run() method with an administrative user.

    This test is a fairly minimal test of the run() method. It does not exercise all the
    potential code paths or provide all the possible run inputs, such as job parameters, cell
    metadata, etc.
    """

    # Set up data variables
    client_group = "grotesquememlong"
    cpus = 4
    mem = "32M"

    # set up mocks
    mocks = _set_up_mocks(_USER, _TOKEN)
    sdkmr = mocks[SDKMethodRunner]
    catutils = mocks[CatalogUtils]
    condor = mocks[Condor]
    # We intentionally do not check the logger methods as there are a lot of them and this is
    # already a very large test. This may be something to be added later when needed.

    # Set up call returns. These calls are in the order they occur in the code
    sdkmr.check_as_admin.return_value = True
    catalog_resources = {
        "client_group": client_group,
        "request_cpus": cpus,
        "request_memory": mem,
    }
    catutils.get_normalized_resources.return_value = catalog_resources
    condor.extract_resources.return_value = CondorResources(
        cpus, "2600GB", mem, client_group
    )
    _set_up_common_return_values(mocks)

    # set up the class to be tested and run the method
    rj = EE2RunJob(sdkmr)
    params = {
        "method": _METHOD,
        "app_id": _APP,
        "source_ws_objects": [_WS_REF_1, _WS_REF_2],
    }
    assert rj.run(params, as_admin=True) == _JOB_ID

    # check mocks called as expected. The order here is the order that they're called in the code.
    sdkmr.check_as_admin.assert_called_once_with(JobPermissions.WRITE)
    catutils.get_normalized_resources.assert_called_once_with(_METHOD)
    condor.extract_resources.assert_called_once_with(catalog_resources)
    expected_condor_resources = CondorResources(4, "2600", "32", client_group)
    _check_common_mock_calls(mocks, catalog_resources, expected_condor_resources, None)


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
