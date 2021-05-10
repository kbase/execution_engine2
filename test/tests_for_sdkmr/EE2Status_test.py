"""
Unit tests for the EE2Status class.
"""

from logging import Logger
from unittest.mock import create_autospec, call
from bson.objectid import ObjectId

from execution_engine2.db.models.models import Job, Status, JobInput
from execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.sdk.EE2Status import JobsStatus, JobPermissions
from execution_engine2.db.MongoUtil import MongoUtil
from lib.execution_engine2.utils.KafkaUtils import KafkaClient, KafkaFinishJob
from lib.execution_engine2.utils.Condor import Condor
from installed_clients.CatalogClient import Catalog


def _finish_job_complete_minimal_get_test_job(job_id, sched, app_id, gitcommit, user):
    job = Job()
    job.id = ObjectId(job_id)
    job.running = 123.0
    job.finished = 456.5
    job.status = Status.running.value
    job.scheduler_id = sched
    job_input = JobInput()
    job.job_input = job_input
    job_input.app_id = app_id
    job_input.method = "module.method_id"
    job_input.service_ver = gitcommit
    job.user = user
    return job


def test_finish_job_complete_minimal_without_app_id():
    _finish_job_complete_minimal(None, None)


def test_finish_job_complete_minimal_with_app_id():
    _finish_job_complete_minimal("module/myapp", "module")


def _finish_job_complete_minimal(app_id, app_module):
    """
    Tests a very simple case of completing a job successfully by the `finish_job` method.
    """
    # set up constants
    job_id = "6046b539ce9c58ecf8c3e5f3"
    job_output = {"version": "1.1", "id": job_id, "result": [{"foo": "bar"}]}
    user = "someuser"
    gitcommit = "somecommit"
    resources = {"fake": "condor", "resources": "in", "here": "yo"}
    sched = "somescheduler"

    # set up mocks
    sdkmr = create_autospec(SDKMethodRunner, spec_set=True, instance=True)
    logger = create_autospec(Logger, spec_set=True, instance=True)
    mongo = create_autospec(MongoUtil, spec_set=True, instance=True)
    kafka = create_autospec(KafkaClient, spec_set=True, instance=True)
    catalog = create_autospec(Catalog, spec_set=True, instance=True)
    condor = create_autospec(Condor, spec_set=True, instance=True)
    sdkmr.get_mongo_util.return_value = mongo
    sdkmr.get_logger.return_value = logger
    sdkmr.get_kafka_client.return_value = kafka
    sdkmr.get_condor.return_value = condor
    sdkmr.get_catalog.return_value = catalog

    # set up return values for mocks. Ordered as per order of operations in code
    job1 = _finish_job_complete_minimal_get_test_job(
        job_id, sched, app_id, gitcommit, user
    )
    job2 = _finish_job_complete_minimal_get_test_job(
        job_id, sched, app_id, gitcommit, user
    )
    job2.status = Status.completed.value

    sdkmr.get_job_with_permission.side_effect = [job1, job2]
    mongo.get_job.return_value = job2  # gets the job 3x...?
    condor.get_job_resource_info.return_value = resources

    # call the method
    JobsStatus(sdkmr).finish_job(job_id, job_output=job_output)  # no return

    # check mocks called as expected. Ordered as per order of operations in code

    sdkmr.get_job_with_permission.assert_has_calls(
        [
            call(
                job_id=job_id, requested_job_perm=JobPermissions.WRITE, as_admin=False
            ),
            call(
                job_id=job_id, requested_job_perm=JobPermissions.WRITE, as_admin=False
            ),
        ]
    )
    logger.debug.assert_has_calls(
        [
            call("Finishing job with a success"),
            # depending on stable dict ordering for this test to pass
            call(f"Extracted the following condor job ads {resources}"),
        ]
    )
    mongo.finish_job_with_success.assert_called_once_with(job_id, job_output)
    kafka.send_kafka_message.assert_called_once_with(
        KafkaFinishJob(
            job_id=job_id,
            new_status=Status.completed.value,
            previous_status=Status.running.value,
            scheduler_id=sched,
            error_code=None,
            error_message=None,
        )
    )
    mongo.get_job.assert_called_once_with(job_id)
    les_expected = {
        "user_id": user,
        "func_module_name": "module",
        "func_name": "method_id",
        "git_commit_hash": gitcommit,
        "creation_time": 1615246649.0,  # from Job ObjectId
        "exec_start_time": 123.0,
        "finish_time": 456.5,
        "is_error": 0,
        "job_id": job_id,
    }
    if app_id:
        les_expected.update({"app_id": app_id, "app_module_name": app_module})
    catalog.log_exec_stats.assert_called_once_with(les_expected)
    mongo.update_job_resources.assert_called_once_with(job_id, resources)
