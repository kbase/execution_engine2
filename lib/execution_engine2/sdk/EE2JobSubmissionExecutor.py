import threading
import time

from execution_engine2.db.models.models import TerminatedCode, Status, ErrorCode
from execution_engine2.sdk.EE2RunjobConstants import _JOB_REQUIREMENTS, _METHOD, _APP_ID, _BATCH_ID, _PARENT_JOB_ID, \
    _WORKSPACE_ID, \
    _SOURCE_WS_OBJECTS, JobIdPair
from execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.sdk.job_submission_parameters import JobSubmissionParameters
from execution_engine2.utils.KafkaUtils import KafkaCreateJob, KafkaQueueChange
from execution_engine2.utils.application_info import AppInfo
from utils.user_info import UserCreds


class EE2JobSubmissionExecutor:
    def __init__(self, sdkmr: SDKMethodRunner):
        self.sdkmr = sdkmr
        self.logger = self.sdkmr.get_logger()

    def generate_job_submission_params(self, job_id, params):
        return JobSubmissionParameters(
            job_id,
            AppInfo(params[_METHOD], params.get(_APP_ID)),
            params[_JOB_REQUIREMENTS],
            UserCreds(self.sdkmr.get_user_id(), self.sdkmr.get_token()),
            # a job should have a parent ID or a batch ID or nothing, but never both
            # Do we want to distinguish between the two cases in the sub params?
            # It's informational only for Condor
            parent_job_id=params.get(_BATCH_ID) or params.get(_PARENT_JOB_ID),
            wsid=params.get(_WORKSPACE_ID),
            source_ws_objects=params.get(_SOURCE_WS_OBJECTS),
        )

    def _submit_multiple_wrapper(self, job_ids, runjob_params):
        # Generate job submission params
        job_submission_params = []
        for i, job_id in enumerate(job_ids):
            job_submission_params.append(
                self.generate_job_submission_params(job_id, runjob_params[i])
            )
            assert job_id == job_submission_params[i].job_id

        # Takes 2.5200018882751465 for 100 records, can shave off 2.5 secs by making this async
        for job_id in job_ids:
            self.sdkmr.get_kafka_client().send_kafka_message(
                message=KafkaCreateJob(
                    job_id=str(job_id), user=self.sdkmr.get_user_id()
                )
            )
        # Submit to Condor
        try:
            submission_ids = self._submit_multiple(job_submission_params)
            return submission_ids
        except Exception as e:
            self.abort_multiple_jobs(job_ids)
            raise e

    def async_submit_multiple(self, runjob_params, job_ids):

        # Start up job submission thread
        # For testing, mock this out and check to see it is called with these params?
        x = threading.Thread(
            target=self._submit_multiple_wrapper,
            kwargs={"runjob_params": runjob_params, "job_ids": job_ids},
            daemon=True,
        )
        x.start()

    def _submit_multiple(self, job_submission_params):
        """
        Submit multiple jobs. If any of the submissions are a failure, raise exception in order
        to fail all submitted jobs, rather than allowing the submissions to continue
        """
        begin = time.time()
        job_ids = []
        condor_job_ids = []
        for job_submit_param in job_submission_params:
            job_id = job_submit_param.job_id
            job_ids.append(job_id)
            try:
                submission_info = self.sdkmr.get_condor().run_job(
                    params=job_submit_param
                )
                condor_job_id = submission_info.clusterid
            except Exception as e:
                self.logger.error(e)
                self.finish_created_job(job_id=job_id, exception=e)
                raise e

            if submission_info.error is not None and isinstance(
                    submission_info.error, Exception
            ):
                self.finish_created_job(exception=submission_info.error, job_id=job_id)
                raise submission_info.error
            if condor_job_id is None:
                error_msg = (
                    "Condor job not run, and error not found. Something went wrong"
                )
                self.finish_created_job(
                    job_id=job_id, exception=RuntimeError(error_msg)
                )
                raise RuntimeError(error_msg)
            condor_job_ids.append(condor_job_id)

        self.logger.error(f"It took {time.time() - begin} to submit jobs to condor")
        # It took 4.836009502410889 to submit jobs to condor

        update_time = time.time()
        self._update_to_queued_multiple(job_ids=job_ids, scheduler_ids=condor_job_ids)
        # It took 1.9239885807037354 to update jobs
        self.logger.error(f"It took {time.time() - update_time} to update jobs ")

        return job_ids

    def abort_multiple_jobs(self, job_ids):
        """
        Cancel a list of child jobs, and their child jobs
        """
        for job_id in job_ids:
            try:
                self.sdkmr.cancel_job(
                    job_id=job_id,
                    terminated_code=TerminatedCode.terminated_by_batch_abort.value,
                )
            except Exception as e:
                # TODO Maybe add a retry here?
                self.logger.error(f"Couldn't cancel child job {e}")

    def _update_to_queued_multiple(self, job_ids, scheduler_ids):
        """
        This is called during job submission. If a job is terminated during job submission,
        we have the chance to re-issue a termination and remove the job from the Job Queue
        """
        if len(job_ids) != len(scheduler_ids):
            raise Exception(
                "Need to provide the same amount of job ids and scheduler_ids"
            )
        jobs_to_update = list(map(JobIdPair, job_ids, scheduler_ids))
        self.sdkmr.get_mongo_util().update_jobs_to_queued(jobs_to_update)
        jobs = self.sdkmr.get_mongo_util().get_jobs(job_ids)

        for job in jobs:
            job_id = str(job.id)
            if job.status == Status.queued.value:
                self.sdkmr.get_kafka_client().send_kafka_message(
                    message=KafkaQueueChange(
                        job_id=job_id,
                        new_status=Status.queued.value,
                        previous_status=Status.created.value,  # TODO maybe change this to allow for estimating jobs
                        scheduler_id=job.scheduler_id,
                    )
                )
            elif job.status == Status.terminated.value:
                # Remove from the queue, now that the scheduler_id is available
                # The job record doesn't actually get updated in the db a 2nd time, and this TerminatedCode is only
                # used by the initial transition to Terminated
                self.sdkmr.cancel_job(job_id=job_id, terminated_code=TerminatedCode.terminated_by_user, )

    def finish_created_job(
            self, job_id, exception, error_code=None, error_message=None
    ):
        """
        :param job_id:
        :param exception:
        :param error_code:
        :param error_message:
        :return:
        """
        if error_message is None:
            error_message = f"Cannot submit to condor {exception}"
        if error_code is None:
            error_code = ErrorCode.job_crashed.value

        # TODO Better Error Parameter? A Dictionary?
        self.sdkmr.finish_job(
            job_id=job_id,
            error_message=error_message,
            error_code=error_code,
            error=f"{exception}",
        )
