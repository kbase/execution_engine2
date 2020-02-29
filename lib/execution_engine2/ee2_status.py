import logging
from collections import OrderedDict
from enum import Enum

from bson import ObjectId

from execution_engine2.authorization.authstrategy import can_read_jobs
from execution_engine2.db.models.models import (
    Job,
    JobOutput,
    Status,
    ErrorCode,
    TerminatedCode,
)
from execution_engine2.exceptions import InvalidStatusTransitionException
from execution_engine2.utils.KafkaUtils import (
    KafkaCancelJob,
    KafkaCondorCommand,
    KafkaFinishJob,
    KafkaStatusChange,
)


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


class JobsStatus:
    def __init__(self, sdkmr):
        self.sdkmr = sdkmr

    def cancel_job(self, job_id, terminated_code=None, as_admin=False):
        """
        Authorization Required: Ability to Read and Write to the Workspace
        Default for terminated code is Terminated By User
        :param job_id:
        :param terminated_code:
        :return:
        """
        # Is it inefficient to get the job twice? Is it cached?
        # Maybe if the call fails, we don't actually cancel the job?
        self.sdkmr.logger.debug(f"Attempting to cancel job {job_id}")

        job = self.sdkmr._get_job_with_permission(job_id, JobPermissions.WRITE)

        if terminated_code is None:
            terminated_code = TerminatedCode.terminated_by_user.value

        self.sdkmr.get_mongo_util().cancel_job(job_id=job_id, terminated_code=terminated_code)

        self.sdkmr.kafka_client.send_kafka_message(
            message=KafkaCancelJob(
                job_id=str(job_id),
                previous_status=job.status,
                new_status=Status.terminated.value,
                scheduler_id=job.scheduler_id,
                terminated_code=terminated_code,
            )
        )

        logging.info(f"About to cancel job in CONDOR using {job.scheduler_id}")
        self.sdkmr.logger.debug(f"About to cancel job in CONDOR using {job.scheduler_id}")
        rv = self.sdkmr.get_condor().cancel_job(job_id=job.scheduler_id)
        logging.info(rv)
        self.sdkmr.logger.debug(f"{rv}")

        self.sdkmr.kafka_client.send_kafka_message(
            message=KafkaCondorCommand(
                job_id=str(job_id),
                scheduler_id=job.scheduler_id,
                condor_command="condor_rm",
            )
        )

    def check_job_canceled(self, job_id, as_admin=False):
        """
        Authorization Required: None
        Check to see if job is terminated by the user
        :return: job_id, whether or not job is canceled, and whether or not job is finished
        """

        job_status = self.sdkmr.get_mongo_util().get_job(job_id=job_id).status
        rv = {"job_id": job_id, "canceled": False, "finished": False}

        if Status(job_status) is Status.terminated:
            rv["canceled"] = True
            rv["finished"] = True

        if Status(job_status) in [Status.completed, Status.error, Status.terminated]:
            rv["finished"] = True
        return rv

    def update_job_status(self, job_id, status):
        """
        #TODO Deprecate this in favor of specific methods with specific checks?
        update_job_status: update status of a job runner record.
                           raise error if job is not found or status is not listed in models.Status
        * Does not update TerminatedCode or ErrorCode
        * Does not update Timestamps
        * Allows invalid state transitions, e.g. Running -> Created

        Parameters:
        job_id: id of job
        """

        if not (job_id and status):
            raise ValueError("Please provide both job_id and status")

        job = self.sdkmr._get_job_with_permission(job_id, JobPermissions.WRITE)

        previous_status = job.status
        job.status = status
        with self.sdkmr.get_mongo_util().mongo_engine_connection():
            job.save()

        self.sdkmr.kafka_client.send_kafka_message(
            message=KafkaStatusChange(
                job_id=str(job_id),
                new_status=status,
                previous_status=previous_status,
                scheduler_id=job.scheduler_id,
            )
        )

        return str(job.id)

    def get_job_status(self, job_id):
        """
        get_job_status: fetch status of a job runner record.
                        raise error if job is not found

        Parameters:
        job_id: id of job

        Returns:
        returnVal: returnVal['status'] status of job
        """

        returnVal = dict()

        if not job_id:
            raise ValueError("Please provide valid job_id")

        job = self.sdkmr._get_job_with_permission(job_id, JobPermissions.READ)

        returnVal["status"] = job.status

        return returnVal

    def _check_job_is_status(self, job_id, status):
        job = self.sdkmr.get_mongo_util().get_job(job_id=job_id)
        if job.status != status:
            raise InvalidStatusTransitionException(
                f"Unexpected job status: {job.status} . Expected {status} "
            )
        return job

    def _check_job_is_created(self, job_id):
        return self.sdkmr._check_job_is_status(job_id, Status.created.value)

    def _check_job_is_running(es, job_id):
        return sdkmr._check_job_is_status(job_id, Status.running.value)

    def _finish_job_with_error(self, job_id, error_message, error_code, error=None):
        if error_code is None:
            error_code = ErrorCode.unknown_error.value

        sdkmr.get_mongo_util().finish_job_with_error(
            job_id=job_id,
            error_message=error_message,
            error_code=error_code,
            error=error,
        )

    def _finish_job_with_success(self, job_id, job_output):
        output = JobOutput()
        output.version = job_output.get("version")
        output.id = ObjectId(job_output.get("id"))
        output.result = job_output.get("result")
        try:
            output.validate()
        except Exception as e:
            logging.info(e)
            error_message = "Something was wrong with the output object"
            error_code = ErrorCode.job_missing_output.value
            error = {
                "code": -1,
                "name": "Output object is invalid",
                "message": str(e),
                "error": str(e),
            }

            sdkmr.get_mongo_util().finish_job_with_error(
                job_id=job_id,
                error_message=error_message,
                error_code=error_code,
                error=error,
            )
            raise Exception(str(e) + str(error_message))

        sdkmr.get_mongo_util().finish_job_with_success(
            job_id=job_id, job_output=job_output
        )

    def finish_job(
            sdkmr, job_id, error_message=None, error_code=None, error=None, job_output=None
    ):
        """
        #TODO Fix too many open connections to mongoengine

        finish_job: set job record to finish status and update finished timestamp
                    (set job status to "finished" by default. If error_message is given, set job to "error" status)
                    raise error if job is not found or current job status is not "running"
                    (general work flow for job status created -> queued -> estimating -> running -> finished/error/terminated)
        Parameters:
        :param job_id: string - id of job
        :param error_message: string - default None, if given set job to error status
        :param error_code: int - default None, if given give this job an error code
        :param error: dict - default None, if given, set the error to this structure
        :param job_output: dict - default None, if given this job has some output
        """

        job = sdkmr._get_job_with_permission(
            job_id=job_id, permission=JobPermissions.WRITE
        )

        if error_message:
            if error_code is None:
                error_code = ErrorCode.job_crashed.value
            db_update = sdkmr._finish_job_with_error(
                job_id=job_id,
                error_message=error_message,
                error_code=error_code,
                error=error,
            )

            sdkmr.kafka_client.send_kafka_message(
                message=KafkaFinishJob(
                    job_id=str(job_id),
                    new_status=Status.error.value,
                    previous_status=job.status,
                    error_message=error_message,
                    error_code=error_code,
                    scheduler_id=job.scheduler_id,
                )
            )
            return db_update

        if job_output is None:
            if error_code is None:
                error_code = ErrorCode.job_missing_output.value
            msg = "Missing job output required in order to successfully finish job. Something went wrong"
            db_update = sdkmr._finish_job_with_error(
                job_id=job_id, error_message=msg, error_code=error_code
            )

            sdkmr.kafka_client.send_kafka_message(
                message=KafkaFinishJob(
                    job_id=str(job_id),
                    new_status=Status.error.value,
                    previous_status=job.status,
                    error_message=msg,
                    error_code=error_code,
                    scheduler_id=job.scheduler_id,
                )
            )
            return db_update

        sdkmr._finish_job_with_success(job_id=job_id, job_output=job_output)
        sdkmr.kafka_client.send_kafka_message(
            message=KafkaFinishJob(
                job_id=str(job_id),
                new_status=Status.completed.value,
                previous_status=job.status,
                scheduler_id=job.scheduler_id,
            )
        )

    def check_job(self, job_id, check_permission=True, exclude_fields=None):

        """
        check_job: check and return job status for a given job_id

        Parameters:
        job_id: id of job
        """

        logging.info("Start fetching status for job: {}".format(job_id))

        if exclude_fields is None:
            exclude_fields = []

        if not job_id:
            raise ValueError("Please provide valid job_id")

        job_state = self.check_jobs(
            [job_id],
            check_permission=check_permission,
            exclude_fields=exclude_fields,
            return_list=0,
        ).get(job_id)

        return job_state

    def check_jobs(
            self, job_ids, check_permission=True, exclude_fields=None, return_list=None
    ):
        """
        check_jobs: check and return job status for a given of list job_ids
        """

        logging.info("Start fetching status for jobs: {}".format(job_ids))

        if exclude_fields is None:
            exclude_fields = []

        with self.sdkmr.get_mongo_util().mongo_engine_connection():
            jobs = self.sdkmr.get_mongo_util().get_jobs(
                job_ids=job_ids, exclude_fields=exclude_fields
            )

        if check_permission:
            try:
                perms = can_read_jobs(jobs, self.sdkmr.user_id, self.sdkmr.token, self.sdkmr.config)
            except RuntimeError as e:
                logging.error(
                    f"An error occurred while checking read permissions for jobs"
                )
                raise e
        else:
            perms = [True] * len(jobs)

        job_states = dict()
        for idx, job in enumerate(jobs):
            if not perms[idx]:
                job_states[str(job.id)] = {"error": "Cannot read this job"}
            mongo_rec = job.to_mongo().to_dict()
            del mongo_rec["_id"]
            mongo_rec["job_id"] = str(job.id)
            mongo_rec["created"] = int(job.id.generation_time.timestamp() * 1000)
            mongo_rec["updated"] = int(job.updated * 1000)
            if job.estimating:
                mongo_rec["estimating"] = int(job.estimating * 1000)
            if job.running:
                mongo_rec["running"] = int(job.running * 1000)
            if job.finished:
                mongo_rec["finished"] = int(job.finished * 1000)
            if job.queued:
                mongo_rec["queued"] = int(job.queued * 1000)

            job_states[str(job.id)] = mongo_rec

        job_states = OrderedDict(
            {job_id: job_states.get(job_id, []) for job_id in job_ids}
        )

        if return_list is not None and self.sdkmr.parse_bool_from_string(
                return_list
        ):
            job_states = {"job_states": list(job_states.values())}

        return job_states

    def check_workspace_jobs(self, workspace_id, exclude_fields=None, return_list=None):
        """
        check_workspace_jobs: check job status for all jobs in a given workspace
        """
        logging.info(
            "Start fetching all jobs status in workspace: {}".format(workspace_id)
        )

        if exclude_fields is None:
            exclude_fields = []

        ws_auth = self.sdkmr.get_workspace_auth()
        if not ws_auth.can_read(workspace_id):
            self.sdkmr.logger.debug(
                f"User {self.sdkmr.user_id} doesn't have permission to read jobs in workspace {workspace_id}."
            )
            raise PermissionError(
                f"User {self.sdkmr.user_id} does not have permission to read jobs in workspace {workspace_id}"
            )

        with self.sdkmr.get_mongo_util().mongo_engine_connection():
            job_ids = [str(job.id) for job in Job.objects(wsid=workspace_id)]

        if not job_ids:
            return {}

        job_states = self.sdkmr.check_jobs(
            job_ids,
            check_permission=False,
            exclude_fields=exclude_fields,
            return_list=return_list,
        )

        return job_states

    @staticmethod
    def _job_state_from_jobs(jobs):
        """
        Returns as per the spec file

        :param jobs: MongoEngine Job Objects Query
        :return: list of job states of format
        Special Cases:
        str(_id)
        str(job_id)
        float(created/queued/estimating/running/finished/updated/) (Time in MS)
        """
        job_states = []
        for job in jobs:
            mongo_rec = job.to_mongo().to_dict()
            mongo_rec["_id"] = str(job.id)
            mongo_rec["job_id"] = str(job.id)
            mongo_rec["created"] = int(job.id.generation_time.timestamp() * 1000)
            mongo_rec["updated"] = int(job.updated * 1000)
            if job.estimating:
                mongo_rec["estimating"] = int(job.estimating * 1000)
            if job.queued:
                mongo_rec["queued"] = int(job.queued * 1000)
            if job.running:
                mongo_rec["running"] = int(job.running * 1000)
            if job.finished:
                mongo_rec["finished"] = int(job.finished * 1000)
            job_states.append(mongo_rec)
        return job_states

    def _send_exec_stats_to_catalog(self, job_id):
        job = self.sdkmr.get_mongo_util().get_job(job_id)

        job_input = job.job_input

        log_exec_stats_params = dict()
        log_exec_stats_params["user_id"] = job.user
        app_id = job_input.app_id
        log_exec_stats_params["app_module_name"] = app_id.split("/")[0]
        log_exec_stats_params["app_id"] = app_id
        method = job_input.method
        log_exec_stats_params["func_module_name"] = method.split(".")[0]
        log_exec_stats_params["func_name"] = method.split(".")[-1]
        log_exec_stats_params["git_commit_hash"] = job_input.service_ver
        log_exec_stats_params["creation_time"] = job.id.generation_time.timestamp()
        log_exec_stats_params["exec_start_time"] = job.running.timestamp()
        log_exec_stats_params["finish_time"] = job.finished.timestamp()
        log_exec_stats_params["is_error"] = int(job.status == Status.error.value)
        log_exec_stats_params["job_id"] = job_id

        self.sdkmr.catalog_utils.catalog.log_exec_stats(log_exec_stats_params)
