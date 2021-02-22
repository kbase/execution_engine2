"""
Unit tests for the EE2Runjob class.
"""

# Incomplete by a long way. Will add more unit tests as they come up.

from bson.objectid import ObjectId
from logging import Logger
from unittest.mock import create_autospec, call
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
    sdkmr.get_token.return_value = "tokentokentoken"
    sdkmr.get_user_id.return_value = "someuser"
    sdkmr.get_workspace.return_value = ws

    job_id = "603051cfaf2e3401b0500982"

    # Set up call returns. These calls are in the order the occur in the code
    sdkmr.check_as_admin.return_value = True
    sdkmr.save_job.return_value = job_id
    ws.get_object_info3.return_value = {"paths": [["1/2/3"], ["4/5/6"]]}
    catalog_resources = {
        "client_group": "grotesquememlong",
        "request_cpus": "4",
        "request_memory": "32",
    }
    catutils.get_normalized_resources.return_value = catalog_resources
    condor.extract_resources.return_value = CondorResources(
        "4", "2600", "32", "grotesquememlong"
    )
    catalog.get_module_version.return_value = {"git_commit_hash": "git5678"}
    condor.run_job.return_value = SubmissionInfo("cluster42", {}, None)
    retjob = Job()
    retjob.id = ObjectId(job_id)
    retjob.status = "created"
    mongo.get_job.return_value = retjob

    # set up the class to be tested and run the method
    rj = EE2RunJob(sdkmr)
    params = {
        "method": "lolcats.lol_unto_death",
        "source_ws_objects": ["1/2/3", "4/5/6"],
    }
    assert rj.run(params, as_admin=True) == job_id

    # check mocks called as expected. The order here is the order that they're called in the code.
    sdkmr.check_as_admin.assert_called_once_with(JobPermissions.WRITE)
    ws.get_object_info3.assert_called_once_with(
        {"objects": [{"ref": "1/2/3"}, {"ref": "4/5/6"}], "ignoreErrors": 1}
    )
    catutils.get_normalized_resources.assert_called_once_with("lolcats.lol_unto_death")
    condor.extract_resources.assert_called_once_with(catalog_resources)
    catalog.get_module_version.assert_called_once_with(
        {"module_name": "lolcats", "version": "release"}
    )

    # initial job data save
    expected_job = Job()
    expected_job.user = "someuser"
    expected_job.status = "created"
    ji = JobInput()
    ji.method = "lolcats.lol_unto_death"
    ji.service_ver = "git5678"
    ji.source_ws_objects = ["1/2/3", "4/5/6"]
    ji.parent_job_id = "None"
    jr = JobRequirements()
    jr.clientgroup = "grotesquememlong"
    jr.cpu = "4"
    jr.memory = "3"
    jr.disk = "26"
    ji.requirements = jr
    ji.narrative_cell_info = Meta()
    expected_job.job_input = ji
    assert len(sdkmr.save_job.call_args_list) == 2
    got_job = sdkmr.save_job.call_args_list[0][0][0]
    assert_jobs_equal(got_job, expected_job)

    kafka.send_kafka_message.assert_any_call(
        KafkaCreateJob("someuser", job_id)
    )
    condor.run_job.assert_called_once_with(
        params={
            "method": "lolcats.lol_unto_death",
            "source_ws_objects": ["1/2/3", "4/5/6"],
            "service_ver": "git5678",
            "job_id": job_id,
            "user_id": "someuser",
            "token": "tokentokentoken",
            "cg_resources_requirements": {
                "client_group": "grotesquememlong",
                "request_cpus": "4",
                "request_memory": "32",
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
    expected_job.status = "queued"
    # no way to test this really without code refactoring
    expected_job.queued = got_job.queued

    expected_job.scheduler_type = "condor"
    expected_job.scheduler_id = "cluster42"
    assert_jobs_equal(got_job, expected_job)

    kafka.send_kafka_message.assert_called_with(  # update to queued state
        KafkaQueueChange(
            job_id=job_id,
            new_status="queued",
            previous_status="created",
            scheduler_id="cluster42",
        )
    )
    slack.run_job_message.assert_called_once_with(
        job_id, "cluster42", "someuser"
    )


def assert_jobs_equal(got_job: Job, expected_job: Job):
    """
    Checks that the two jobs are equivalent, except that the 'updated' fields are checked that
    they're within 1 second of each other.
    """
    # I could not get assert got_job == expected_job to ever work, or even
    # JobRequirements1 == JobRequirements2 to work when their fields were identical, so we
    # do this disgusting hack instead. Note it will need to be updated any time a job field is
    # added.
    if not hasattr(got_job, "id"):
        assert not hasattr(expected_job, "id")
    else:
        assert got_job.id == expected_job.id
    assert got_job.user == expected_job.user
    assert got_job.authstrat == expected_job.authstrat
    assert got_job.wsid == expected_job.wsid
    assert got_job.status == expected_job.status

    # The Job class fills the updated field with the output of time.time on instantiation
    # so we can't do a straight equality
    assert abs(got_job.updated - expected_job.updated) < 1

    assert got_job.queued == expected_job.queued
    assert got_job.estimating == expected_job.estimating
    assert got_job.running == expected_job.running
    assert got_job.finished == expected_job.finished
    assert got_job.errormsg == expected_job.errormsg
    assert got_job.msg == expected_job.msg
    assert got_job.error == expected_job.error
    assert got_job.terminated_code == expected_job.terminated_code
    assert got_job.error_code == expected_job.error_code
    assert got_job.scheduler_type == expected_job.scheduler_type
    assert got_job.scheduler_id == expected_job.scheduler_id
    assert got_job.scheduler_estimator_id == expected_job.scheduler_estimator_id

    if not got_job.job_input:
        assert expected_job.job_input is None
    else:
        assert got_job.job_input.wsid == expected_job.job_input.wsid
        assert got_job.job_input.method == expected_job.job_input.method
        # Watch out for tuples here: https://dbader.org/blog/catching-bogus-python-asserts
        assert (
            got_job.job_input.requested_release
            == expected_job.job_input.requested_release
        )
        assert got_job.job_input.params == expected_job.job_input.params
        assert got_job.job_input.service_ver == expected_job.job_input.service_ver
        assert got_job.job_input.app_id == expected_job.job_input.app_id
        assert (
            got_job.job_input.source_ws_objects
            == expected_job.job_input.source_ws_objects
        )
        assert got_job.job_input.parent_job_id == expected_job.job_input.parent_job_id

        assert got_job.job_input.requirements.clientgroup == (
            expected_job.job_input.requirements.clientgroup
        )
        assert (
            got_job.job_input.requirements.cpu
            == expected_job.job_input.requirements.cpu
        )
        assert (
            got_job.job_input.requirements.memory
            == expected_job.job_input.requirements.memory
        )
        assert (
            got_job.job_input.requirements.disk
            == expected_job.job_input.requirements.disk
        )
        assert got_job.job_input.requirements.estimate == (
            expected_job.job_input.requirements.estimate
        )

        # this fails, which should be impossible given all the fields above pass
        # assert got_job.job_input.requirements == expected_job.job_input.requirements

        assert got_job.job_input.narrative_cell_info.run_id == (
            expected_job.job_input.narrative_cell_info.run_id
        )
        assert got_job.job_input.narrative_cell_info.token_id == (
            expected_job.job_input.narrative_cell_info.token_id
        )
        assert got_job.job_input.narrative_cell_info.tag == (
            expected_job.job_input.narrative_cell_info.tag
        )
        assert got_job.job_input.narrative_cell_info.cell_id == (
            expected_job.job_input.narrative_cell_info.cell_id
        )
        assert got_job.job_input.narrative_cell_info.status == (
            expected_job.job_input.narrative_cell_info.status
        )

        # again, this fails, even though the fields are all ==
        # assert got_job.job_input.narrative_cell_info == (
        #     expected_job.job_input.narrative_cell_info)

    assert got_job.job_output == expected_job.job_output
    assert got_job.condor_job_ads == expected_job.condor_job_ads
    assert got_job.child_jobs == expected_job.child_jobs
    assert got_job.batch_job == expected_job.batch_job
