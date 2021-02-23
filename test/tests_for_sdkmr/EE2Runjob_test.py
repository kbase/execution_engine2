"""
Unit tests for the EE2Runjob class.
"""

# Incomplete by a long way. Will add more unit tests as they come up.

from bson.objectid import ObjectId
from logging import Logger
from unittest.mock import create_autospec
from execution_engine2.db.models.models import Job, JobInput, JobRequirements, Meta
from execution_engine2.sdk.EE2Runjob import EE2RunJob, JobPermissions
from execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.utils.CatalogUtils import CatalogUtils
from execution_engine2.utils.Condor import (
    Condor,
    CondorResources,
    SubmissionInfo,
)
from execution_engine2.utils.KafkaUtils import (
    KafkaClient,
    KafkaQueueChange,
    KafkaCreateJob,
)
from execution_engine2.utils.SlackUtils import SlackClient
from lib.execution_engine2.db.MongoUtil import MongoUtil
from installed_clients.WorkspaceClient import Workspace
from installed_clients.CatalogClient import Catalog


def test_run_as_admin():
    """
    A basic unit test of the run() method with an administrative user.

    This test is a fairly minimal test of the run() method. It does not exercise all the
    potential code paths or provide all the possible run inputs, such as job parameters, cell
    metadata, etc.
    """

    # Set up data variables
    job_id = "603051cfaf2e3401b0500982"
    git_commit = "git5678"
    ws_obj1 = "1/2/3"
    ws_obj2 = "4/5/6"
    client_group = "grotesquememlong"
    cpus = "4"
    mem = "32M"
    cluster = "cluster42"
    method = "lolcats.lol_unto_death"
    user = "someuser"
    token = "tokentokentoken"
    created_state = "created"
    queued_state = "queued"

    # The amount of mocking required here implies the method should be broken up into smaller
    # classes that are individually mockable. Or maybe it's just really complicated and this
    # is the best we can do. Worth looking into at some point though.

    # We intentionally do not check the logger methods as there are a lot of them and this is
    # already a very large test. This may be something to be added later when needed.
    sdkmr = create_autospec(SDKMethodRunner, spec_set=True, instance=True)
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    catutils = create_autospec(CatalogUtils, spec_set=True, instance=True)
    condor = create_autospec(Condor, spec_set=True, instance=True)
    kafka = create_autospec(KafkaClient, spec_set=True, instance=True)
    logger = create_autospec(Logger, spec_set=True, instance=True)
    mongo = create_autospec(MongoUtil, spec_set=True, instance=True)
    slack = create_autospec(SlackClient, spec_set=True, instance=True)
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    # Set up basic getter calls
    catutils.get_catalog.return_value = catalog
    sdkmr.get_catalog_utils.return_value = catutils
    sdkmr.get_condor.return_value = condor
    sdkmr.get_kafka_client.return_value = kafka
    sdkmr.get_logger.return_value = logger
    sdkmr.get_mongo_util.return_value = mongo
    sdkmr.get_slack_client.return_value = slack
    sdkmr.get_token.return_value = token
    sdkmr.get_user_id.return_value = user
    sdkmr.get_workspace.return_value = ws

    # Set up call returns. These calls are in the order they occur in the code
    sdkmr.check_as_admin.return_value = True
    sdkmr.save_job.return_value = job_id
    ws.get_object_info3.return_value = {"paths": [[ws_obj1], [ws_obj2]]}
    catalog_resources = {
        "client_group": client_group,
        "request_cpus": cpus,
        "request_memory": mem,
    }
    catutils.get_normalized_resources.return_value = catalog_resources
    condor.extract_resources.return_value = CondorResources(
        cpus, "2600GB", mem, client_group
    )
    catalog.get_module_version.return_value = {"git_commit_hash": git_commit}
    condor.run_job.return_value = SubmissionInfo(cluster, {}, None)
    retjob = Job()
    retjob.id = ObjectId(job_id)
    retjob.status = created_state
    mongo.get_job.return_value = retjob

    # set up the class to be tested and run the method
    rj = EE2RunJob(sdkmr)
    params = {
        "method": method,
        "source_ws_objects": [ws_obj1, ws_obj2],
    }
    assert rj.run(params, as_admin=True) == job_id

    # check mocks called as expected. The order here is the order that they're called in the code.
    sdkmr.check_as_admin.assert_called_once_with(JobPermissions.WRITE)
    ws.get_object_info3.assert_called_once_with(
        {"objects": [{"ref": ws_obj1}, {"ref": ws_obj2}], "ignoreErrors": 1}
    )
    catutils.get_normalized_resources.assert_called_once_with(method)
    condor.extract_resources.assert_called_once_with(catalog_resources)
    catalog.get_module_version.assert_called_once_with(
        {"module_name": "lolcats", "version": "release"}
    )

    # initial job data save
    expected_job = Job()
    expected_job.user = user
    expected_job.status = created_state
    ji = JobInput()
    ji.method = method
    ji.service_ver = git_commit
    ji.source_ws_objects = [ws_obj1, ws_obj2]
    ji.parent_job_id = "None"
    jr = JobRequirements()
    jr.clientgroup = client_group
    jr.cpu = cpus
    jr.memory = "32"
    jr.disk = "2600"
    ji.requirements = jr
    ji.narrative_cell_info = Meta()
    expected_job.job_input = ji
    assert len(sdkmr.save_job.call_args_list) == 2
    got_job = sdkmr.save_job.call_args_list[0][0][0]
    assert_jobs_equal(got_job, expected_job)

    kafka.send_kafka_message.assert_any_call(KafkaCreateJob(user, job_id))
    condor.run_job.assert_called_once_with(
        params={
            "method": method,
            "source_ws_objects": [ws_obj1, ws_obj2],
            "service_ver": git_commit,
            "job_id": job_id,
            "user_id": user,
            "token": token,
            "cg_resources_requirements": {
                "client_group": client_group,
                "request_cpus": cpus,
                "request_memory": mem,
            },
        },
        concierge_params=None,
    )

    # updated job data save
    mongo.get_job.assert_called_once_with(job_id)

    # update to queued state
    got_job = sdkmr.save_job.call_args_list[1][0][0]
    expected_job = Job()
    expected_job.id = ObjectId(job_id)
    expected_job.status = queued_state
    # no way to test this really without code refactoring
    expected_job.queued = got_job.queued

    expected_job.scheduler_type = "condor"
    expected_job.scheduler_id = cluster
    assert_jobs_equal(got_job, expected_job)

    kafka.send_kafka_message.assert_called_with(  # update to queued state
        KafkaQueueChange(
            job_id=job_id,
            new_status=queued_state,
            previous_status=created_state,
            scheduler_id=cluster,
        )
    )
    slack.run_job_message.assert_called_once_with(job_id, cluster, user)


def assert_jobs_equal(got_job: Job, expected_job: Job):
    """
    Checks that the two jobs are equivalent, except that the 'updated' fields are checked that
    they're within 1 second of each other.
    """
    # I could not get assert got_job == expected_job to ever work, or even
    # JobRequirements1 == JobRequirements2 to work when their fields were identical.
    # Also accessing the instance dict via vars() or __dict__ only produced {'_cls': 'Job'} so
    # apparently MongoEngine does something very odd with the dict.
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
        "job_output",
        "condor_job_ads",
        "child_jobs",
        "batch_job",
    ]

    _super_hacky_equals(got_job, expected_job, job_fields)

    if not got_job.job_input:
        assert expected_job.job_input is None
    else:
        job_input_fields = [
            "wsid",
            "method",
            "requested_release",
            "params",
            "service_ver",
            "app_id",
            "source_ws_objects",
            "parent_job_id",
        ]
        _super_hacky_equals(got_job.job_input, expected_job.job_input, job_input_fields)

        requirements_fields = ["clientgroup", "cpu", "memory", "disk", "estimate"]
        _super_hacky_equals(
            got_job.job_input.requirements,
            expected_job.job_input.requirements,
            requirements_fields,
        )
        # this fails, which should be impossible given all the fields above pass
        # assert got_job.job_input.requirements == expected_job.job_input.requirements

        cell_info_fields = ["run_id", "token_id", "tag", "cell_id", "status"]
        _super_hacky_equals(
            got_job.job_input.narrative_cell_info,
            expected_job.job_input.narrative_cell_info,
            cell_info_fields,
        )
        # again, this fails, even though the fields are all ==
        # assert got_job.job_input.narrative_cell_info == (
        #     expected_job.job_input.narrative_cell_info)


def _super_hacky_equals(obj1, obj2, fields):
    for field in fields:
        assert getattr(obj1, field) == getattr(obj2, field), field
