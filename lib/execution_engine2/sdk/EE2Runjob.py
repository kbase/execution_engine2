"""
Authors @bsadkhin, @tgu
All functions related to running a job, and starting a job, including the initial state change and
the logic to retrieve info needed by the runnner to start the job

"""
import os
import threading
import time
from collections import Counter
from enum import Enum
from typing import Optional, Dict, NamedTuple, Union, List, Any

from execution_engine2.db.models.models import (
    Job,
    JobInput,
    Meta,
    JobRequirements,
    Status,
    ErrorCode,
    TerminatedCode,
)
from execution_engine2.exceptions import (
    IncorrectParamsException,
    AuthError,
    CannotRetryJob,
    RetryFailureException,
    InvalidParameterForBatch,
)
from execution_engine2.sdk.EE2Constants import CONCIERGE_CLIENTGROUP
from execution_engine2.sdk.job_submission_parameters import (
    JobSubmissionParameters,
    JobRequirements as ResolvedRequirements,
    AppInfo,
    UserCreds,
)
from execution_engine2.utils.KafkaUtils import KafkaCreateJob, KafkaQueueChange
from execution_engine2.utils.job_requirements_resolver import (
    REQUEST_CPUS,
    REQUEST_DISK,
    REQUEST_MEMORY,
    CLIENT_GROUP,
    CLIENT_GROUP_REGEX,
    BILL_TO_USER,
    IGNORE_CONCURRENCY_LIMITS,
    DEBUG_MODE,
)
from execution_engine2.utils.job_requirements_resolver import RequirementsType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner  # noqa: F401

_JOB_REQUIREMENTS = "job_reqs"
_JOB_REQUIREMENTS_INCOMING = "job_requirements"
_SCHEDULER_REQUIREMENTS = "scheduler_requirements"
_META = "meta"  # narrative_cell_info
_APP_PARAMS = "params"  # application parameters
_REQUIREMENTS_LIST = "requirements_list"
_METHOD = "method"
_APP_ID = "app_id"
_BATCH_ID = "batch_id"
_PARENT_JOB_ID = "parent_job_id"
_PARENT_RETRY_JOB_ID = "retry_parent"
_RETRY_IDS = "retry_ids"
_WORKSPACE_ID = "wsid"
_SOURCE_WS_OBJECTS = "source_ws_objects"
_SERVICE_VER = "service_ver"


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


class PreparedJobParams(NamedTuple):
    params: dict
    job_id: str


class EE2RunJob:
    def __init__(self, sdkmr):
        self.sdkmr = sdkmr  # type: SDKMethodRunner
        self.override_clientgroup = os.environ.get("OVERRIDE_CLIENT_GROUP", None)
        self.logger = self.sdkmr.get_logger()

    def _init_job_rec(
        self, user_id: str, params: Dict, save: bool = True
    ) -> Union[str, Job]:
        f"""
        Save an initial job record to the db and send a message to kafka

        *** Expected OPTIONAL Parameters ***
        {_WORKSPACE_ID} (The workspace id)
        {_APP_PARAMS} (job params for the app/method itself)
        {_SERVICE_VER} (app version)
        {_APP_ID} (app UI)
        {_SOURCE_WS_OBJECTS} (collected workspace objects for this app)
        {_BATCH_ID} (parent of the job for EE2 batch jobs, the parent should be updated)
        {_PARENT_JOB_ID} (parent of this job, doesn't update/notify the parent)
        {_META} (narrative cell information)

        *** Expected REQUIRED Parameters ***
        {_METHOD} (The app method to run)
        {_JOB_REQUIREMENTS} (Job Resource information)
        """
        job = Job()
        inputs = JobInput()
        job.user = user_id
        job.authstrat = "kbaseworkspace"
        job.wsid = params.get(_WORKSPACE_ID)
        job.status = "created"
        # Inputs
        inputs.wsid = job.wsid

        required_job_inputs = [_JOB_REQUIREMENTS, _METHOD]
        for item in required_job_inputs:
            if item not in params:
                raise ValueError(f"{item} is required for job initialization")

        inputs.method = params[_METHOD]
        inputs.params = params.get("params")

        # Catalog git commit
        params[_SERVICE_VER] = self.sdkmr.get_catalog_cache().lookup_git_commit_version(
            method=params.get(_METHOD), service_ver=params.get(_SERVICE_VER)
        )
        inputs.service_ver = params.get(_SERVICE_VER)
        inputs.app_id = params.get(_APP_ID)
        inputs.source_ws_objects = params.get(_SOURCE_WS_OBJECTS)

        parent_job_id = params.get(_PARENT_JOB_ID)
        if parent_job_id:
            inputs.parent_job_id = str(parent_job_id)

        inputs.narrative_cell_info = Meta()

        # Meta and Requirements
        meta = params.get(_META)
        if meta:
            for meta_attr in ["run_id", "token_id", "tag", "cell_id"]:
                inputs.narrative_cell_info[meta_attr] = meta.get(meta_attr)
        resolved_reqs = params[_JOB_REQUIREMENTS]  # type: ResolvedRequirements
        jr = JobRequirements(
            cpu=resolved_reqs.cpus,
            memory=resolved_reqs.memory_MB,
            disk=resolved_reqs.disk_GB,
            clientgroup=resolved_reqs.client_group,
        )
        inputs.requirements = jr
        job.job_input = inputs

        f"""
        Set the id of the parent that was retried to get this job
        The {_PARENT_RETRY_JOB_ID} will only be set on a job retry
        """
        parent_retry_job_id = params.get(_PARENT_RETRY_JOB_ID)
        if parent_retry_job_id:
            job.retry_parent = str(parent_retry_job_id)
        job.batch_id = str(params.get(_BATCH_ID)) if params.get(_BATCH_ID) else None

        if save:
            job_id = self.sdkmr.save_job(job)
            self.sdkmr.get_kafka_client().send_kafka_message(
                message=KafkaCreateJob(job_id=job_id, user=user_id)
            )
            return job_id
        return job

    def _check_ws_objects(self, source_objects) -> None:
        """
        perform sanity checks on input WS objects
        """

        if source_objects:
            objects = [{"ref": ref} for ref in source_objects]
            info = self.sdkmr.get_workspace().get_object_info3(
                {"objects": objects, "ignoreErrors": 1}
            )
            paths = info.get("paths")

            # TODO It would be nice to show which object is inaccessible
            if None in paths:
                raise ValueError("Some workspace object is inaccessible")

    def _check_workspace_permissions(self, wsid):
        if wsid:
            if not self.sdkmr.get_workspace_auth().can_write(wsid):
                self.logger.debug(
                    f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {wsid}."
                )
                raise PermissionError(
                    f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {wsid}."
                )

    def _check_workspace_permissions_list(self, wsids):
        # TODO Cover this in tests once you can execute multiple independent runs
        unique_not_none_not_zero_wsids = [wsid for wsid in set(wsids) if wsid]
        if unique_not_none_not_zero_wsids:
            perms = self.sdkmr.get_workspace_auth().can_write_list(
                unique_not_none_not_zero_wsids
            )
            bad_ws = [key for key in perms.keys() if perms[key] is False]
            if bad_ws:
                self.logger.debug(
                    f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {bad_ws}."
                )
                raise PermissionError(
                    f"User {self.sdkmr.user_id} doesn't have permission to run jobs in workspace {bad_ws}."
                )

    def _finish_created_job(
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

    def _generate_job_submission_params(self, job_id, params):
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

    def _prepare_to_run(self, params, concierge_params=None) -> JobSubmissionParameters:
        """
        Creates a job record and creates the job submission params
        """

        job_id = self._init_job_rec(self.sdkmr.get_user_id(), params)
        self.logger.debug(
            f"User {self.sdkmr.get_user_id()} attempting to run job {params[_METHOD]} {params}"
        )
        return self._generate_job_submission_params(job_id, params)

    def _submit_multiple_wrapper(self, job_ids: list, runjob_params: List[Dict]):
        # Generate job submission params
        job_submission_params = []
        for i, job_id in enumerate(job_ids):
            job_submission_params.append(
                self._generate_job_submission_params(job_id, runjob_params[i])
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
            self._abort_multiple_jobs(job_ids)
            raise e

    def _run_multiple(self, runjob_params: List[Dict]):
        """
        Get the job records, bulk save them, then submit to condor.
        If any condor submission fails, abort all of the jobs
        :return:
        """
        # Save records to db
        job_records = []
        for runjob_param in runjob_params:
            job_records.append(
                self._init_job_rec(self.sdkmr.get_user_id(), runjob_param, save=False)
            )
        job_ids = self.sdkmr.save_jobs(job_records)

        # Start up job submission thread
        # For testing, mock this out and check to see it is called with these params?
        threading.Thread(
            target=self._submit_multiple_wrapper,
            kwargs={"runjob_params": runjob_params, "job_ids": job_ids},
            daemon=True,
        ).start()
        return job_ids

    def _finish_multiple_job_submission(self, job_ids):
        """
        This is called during job submission. If a job is terminated during job submission,
        we have the chance to re-issue a termination and remove the job from the Job Queue
        """
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
                self._safe_cancel(job_id, TerminatedCode.terminated_by_user)

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
                self._finish_created_job(job_id=job_id, exception=e)
                raise e

            if submission_info.error is not None and isinstance(
                submission_info.error, Exception
            ):
                self._finish_created_job(exception=submission_info.error, job_id=job_id)
                raise submission_info.error
            if condor_job_id is None:
                error_msg = (
                    "Condor job not run, and error not found. Something went wrong"
                )
                self._finish_created_job(
                    job_id=job_id, exception=RuntimeError(error_msg)
                )
                raise RuntimeError(error_msg)
            condor_job_ids.append(condor_job_id)
            # Previously the jobs were updated in a batch after submitting all jobs to condor.
            # This led to issues where a large job count could result in jobs switching to
            # running prior to all jobs being submitted and so the queued timestamp was
            # never added to the job record.
            self.sdkmr.get_mongo_util().update_job_to_queued(job_id, condor_job_id)

        self.logger.error(
            f"It took {time.time() - begin} to submit jobs to condor and update to queued"
        )

        update_time = time.time()
        self._finish_multiple_job_submission(job_ids=job_ids)
        self.logger.error(
            f"It took {time.time() - update_time} to finish job submission"
        )

        return job_ids

    def _run(self, params):
        job_params = self._prepare_to_run(params=params)
        job_id = job_params.job_id

        try:
            submission_info = self.sdkmr.get_condor().run_job(params=job_params)
            condor_job_id = submission_info.clusterid
        except Exception as e:
            self.logger.error(e)
            self._finish_created_job(job_id=job_id, exception=e)
            raise e

        if submission_info.error is not None and isinstance(
            submission_info.error, Exception
        ):
            self._finish_created_job(exception=submission_info.error, job_id=job_id)
            raise submission_info.error
        if condor_job_id is None:
            error_msg = "Condor job not run, and error not found. Something went wrong"
            self._finish_created_job(job_id=job_id, exception=RuntimeError(error_msg))
            raise RuntimeError(error_msg)

        self.update_job_to_queued(job_id=job_id, scheduler_id=condor_job_id)

        return job_id

    def _abort_multiple_jobs(self, job_ids):
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

    def _create_batch_job(self, wsid, meta):
        """
        This creates the parent job for all children to mark as their ancestor
        :param params:
        :return:
        """
        job_input = JobInput()
        job_input.service_ver = "batch"
        job_input.app_id = "batch"
        job_input.method = "batch"
        job_input.narrative_cell_info = Meta()

        if meta:
            job_input.narrative_cell_info.run_id = meta.get("run_id")
            job_input.narrative_cell_info.token_id = meta.get("token_id")
            job_input.narrative_cell_info.tag = meta.get("tag")
            job_input.narrative_cell_info.cell_id = meta.get("cell_id")

        j = Job(
            job_input=job_input,
            batch_job=True,
            status=Status.created.value,
            wsid=wsid,
            user=self.sdkmr.get_user_id(),
        )
        j = self.sdkmr.save_and_return_job(j)

        # TODO Do we need a new kafka call for batch?
        self.sdkmr.get_kafka_client().send_kafka_message(
            message=KafkaCreateJob(job_id=str(j.id), user=j.user)
        )
        return j

    def _run_batch(self, batch_job: Job, params):
        """Add the batch id, save the jobs to the db, run the jobs"""

        for job_param in params:
            job_param[_BATCH_ID] = str(batch_job.id)

        child_jobs = self._run_multiple(params)

        # Cancel child jobs if we can't notify the batch job of the child jobs
        try:
            self.sdkmr.add_child_jobs(batch_job=batch_job, child_jobs=child_jobs)
        except Exception as e:
            self._abort_multiple_jobs(child_jobs)
            raise e

        return child_jobs

    def run_batch(
        self, params, batch_params, as_admin=False
    ) -> Dict[str, Union[Job, List[str]]]:
        """
        Warning: modifies params in place
        :param params: List of RunJobParams (See Spec File)
        :param batch_params: Mapping of Batch Params, such as {wsid, as_admin} (See Spec file)
        :param as_admin: Allows you to run jobs in other people's workspaces
        :return: A list of condor job ids or a failure notification
        """

        if not isinstance(params, list):
            raise IncorrectParamsException("params must be a list")

        if not isinstance(batch_params, dict):
            raise IncorrectParamsException("batch params must be a mapping")

        wsid = batch_params.get(_WORKSPACE_ID)
        meta = batch_params.get(_META)

        self._preflight(
            runjob_params=params,
            batch_params=batch_params,
            new_batch_job=True,
            as_admin=as_admin,
        )
        self._add_job_requirements(params, bool(as_admin))  # as_admin checked above
        self._check_job_arguments(params, batch_job=True)
        batch_job = self._create_batch_job(wsid=wsid, meta=meta)
        children_jobs = self._run_batch(batch_job=batch_job, params=params)

        return {_BATCH_ID: str(batch_job.id), "child_job_ids": children_jobs}

    # modifies the jobs in place
    def _add_job_requirements(self, jobs: List[Dict[str, Any]], is_write_admin: bool):
        f"""
        Adds the job requirements, generated from the job requirements resolver,
        to the provided RunJobParams dicts. Expects the required field {_METHOD} in the param
        dicts. Looks in the {_JOB_REQUIREMENTS_INCOMING} key for a dictionary containing the
        optional keys {REQUEST_CPUS}, {REQUEST_MEMORY}, {REQUEST_DISK}, {CLIENT_GROUP},
        {CLIENT_GROUP_REGEX}, {BILL_TO_USER}, {IGNORE_CONCURRENCY_LIMITS},
        {_SCHEDULER_REQUIREMENTS}, and {DEBUG_MODE}. Adds the {_JOB_REQUIREMENTS} field to the
        param dicts, which holds the job requirements object.
        """
        # could add a cache in the job requirements resolver to avoid making the same
        # catalog call over and over if all the jobs have the same method
        jrr = self.sdkmr.get_job_requirements_resolver()
        for i, job in enumerate(jobs):
            # TODO I feel like a class for just handling error formatting would be useful
            # but too much work for a minor benefit
            pre = f"Job #{i + 1}: " if len(jobs) > 1 else ""
            job_reqs = job.get(_JOB_REQUIREMENTS_INCOMING) or {}
            if not isinstance(job_reqs, dict):
                raise IncorrectParamsException(
                    f"{pre}{_JOB_REQUIREMENTS_INCOMING} must be a mapping"
                )
            try:
                norm = jrr.normalize_job_reqs(job_reqs, "input job")
            except IncorrectParamsException as e:
                self._rethrow_incorrect_params_with_error_prefix(e, pre)
            self._check_job_requirements_vs_admin(
                jrr, norm, job_reqs, is_write_admin, pre
            )

            try:
                job[_JOB_REQUIREMENTS] = jrr.resolve_requirements(
                    method=job.get(_METHOD),
                    catalog_cache=self.sdkmr.get_catalog_cache(),
                    cpus=norm.get(REQUEST_CPUS),
                    memory_MB=norm.get(REQUEST_MEMORY),
                    disk_GB=norm.get(REQUEST_DISK),
                    client_group=norm.get(CLIENT_GROUP),
                    client_group_regex=norm.get(CLIENT_GROUP_REGEX),
                    bill_to_user=job_reqs.get(BILL_TO_USER),
                    ignore_concurrency_limits=bool(
                        job_reqs.get(IGNORE_CONCURRENCY_LIMITS)
                    ),
                    scheduler_requirements=job_reqs.get(_SCHEDULER_REQUIREMENTS),
                    debug_mode=norm.get(DEBUG_MODE),
                )
            except IncorrectParamsException as e:
                self._rethrow_incorrect_params_with_error_prefix(e, pre)

    def _check_job_requirements_vs_admin(
        self, jrr, norm, job_reqs, is_write_admin, err_prefix
    ):
        # just a helper method for _add_job_requirements to make that method a bit shorter.
        # treat it as part of that method
        try:
            perm_type = jrr.get_requirements_type(
                cpus=norm.get(REQUEST_CPUS),
                memory_MB=norm.get(REQUEST_MEMORY),
                disk_GB=norm.get(REQUEST_DISK),
                client_group=norm.get(CLIENT_GROUP),
                client_group_regex=norm.get(CLIENT_GROUP_REGEX),
                # Note that this is never confirmed to be a real user. May want to fix that, but
                # since it's admin only... YAGNI
                bill_to_user=self._check_is_string(
                    job_reqs.get(BILL_TO_USER), "bill_to_user"
                ),
                ignore_concurrency_limits=bool(job_reqs.get(IGNORE_CONCURRENCY_LIMITS)),
                scheduler_requirements=job_reqs.get(_SCHEDULER_REQUIREMENTS),
                debug_mode=norm.get(DEBUG_MODE),
            )
        except IncorrectParamsException as e:
            self._rethrow_incorrect_params_with_error_prefix(e, err_prefix)
        if perm_type != RequirementsType.STANDARD and not is_write_admin:
            raise AuthError(
                f"{err_prefix}In order to specify job requirements you must be a full admin"
            )

    def _check_is_string(self, putative_str, name):
        if not putative_str:
            return None
        if not isinstance(putative_str, str):
            raise IncorrectParamsException(f"{name} must be a string")
        return putative_str

    def _rethrow_incorrect_params_with_error_prefix(
        self, error: IncorrectParamsException, error_prefix: str
    ):
        if not error_prefix:
            raise error
        raise IncorrectParamsException(f"{error_prefix}{error.args[0]}") from error

    def _check_job_arguments(self, jobs, batch_job=False):
        # perform sanity checks before creating any jobs, including the parent job for batch jobs
        for i, job in enumerate(jobs):
            # Could make an argument checker method, or a class that doesn't require a job id.
            # Seems like more code & work for no real benefit though.
            # Just create the class for checks, don't use yet
            pre = f"Job #{i + 1}: " if len(jobs) > 1 else ""
            try:
                JobSubmissionParameters(
                    "fakejobid",
                    AppInfo(job.get(_METHOD), job.get(_APP_ID)),
                    job[_JOB_REQUIREMENTS],
                    UserCreds(self.sdkmr.get_user_id(), self.sdkmr.get_token()),
                    wsid=job.get(_WORKSPACE_ID),
                    source_ws_objects=job.get(_SOURCE_WS_OBJECTS),
                )
            except IncorrectParamsException as e:
                self._rethrow_incorrect_params_with_error_prefix(e, pre)
            if batch_job and job.get(_PARENT_JOB_ID):
                raise IncorrectParamsException(
                    f"{pre}batch jobs may not specify a parent job ID"
                )
            # This is also an opportunity for caching
            # although most likely jobs aren't operating on the same object
            self._check_ws_objects(source_objects=job.get(_SOURCE_WS_OBJECTS))

    @staticmethod
    def _retryable(status: str):
        return status in [Status.terminated.value, Status.error.value]

    def _safe_cancel(
        self,
        job_id: str,
        terminated_code: TerminatedCode,
    ):
        try:
            self.sdkmr.cancel_job(job_id=job_id, terminated_code=terminated_code.value)
        except Exception as e:
            self.logger.error(f"Couldn't cancel {job_id} due to {e}")

    def _db_update_failure(
        self, job_that_failed_operation: str, job_to_abort: str, exception: Exception
    ):
        """Attempt to cancel created/queued/running retried job and then raise exception"""
        # TODO Use and create a method in sdkmr?
        msg = (
            f"Couldn't update job record:{job_that_failed_operation} during retry. Aborting:{job_to_abort}"
            f" Exception:{exception} "
        )
        self._safe_cancel(
            job_id=job_to_abort,
            terminated_code=TerminatedCode.terminated_by_server_failure,
        )
        # TODO Maybe move this log into multiple so not multiple error messages are generated
        self.logger.error(msg, exc_info=True, stack_info=True)
        raise RetryFailureException(msg)

    def _validate_retry_presubmit(self, job_id: str, as_admin: bool = False):
        """
        Validate retry request before attempting to contact scheduler

        _validate doesn't do a recursive check if if the job has a retry parent,
        but the _validate call on the recursion is guaranteed to pass because
        the parent was retried once already so the _validate must have passed previously.
        Since the parent job's state can't have changed it would just pass again.
        """

        # Check to see if you still have permissions to the job and then optionally the parent job id
        job = self.sdkmr.get_job_with_permission(
            job_id, JobPermissions.WRITE, as_admin=as_admin
        )  # type: Job

        batch_job = None
        if job.batch_id:
            batch_job = self.sdkmr.get_job_with_permission(
                job.batch_id, JobPermissions.WRITE, as_admin=as_admin
            )

        if job.batch_job:
            raise CannotRetryJob(
                "Cannot retry batch job parents. Must retry individual jobs"
            )

        if not self._retryable(job.status):
            raise CannotRetryJob(
                f"Error retrying job {job_id} with status {job.status}: can only retry jobs with status 'error' or 'terminated'"
            )

        return job, batch_job

    def _retry(self, job_id: str, job: Job, batch_job: Job, as_admin: bool = False):
        # Cannot retry a retried job, you must retry the retry_parent
        if job.retry_parent:
            return self.retry(str(job.retry_parent), as_admin=as_admin)

        # Get run job params from db, and inject parent job id, then run it
        run_job_params = self._get_run_job_params_from_existing_job(
            job, user_id=self.sdkmr.user_id
        )
        # Submit job to job scheduler or fail and not count it as a retry attempt
        run_job_params[_PARENT_RETRY_JOB_ID] = job_id
        retry_job_id = self.run(params=run_job_params, as_admin=as_admin)

        # Save that the job has been retried, and increment the count. Notify the parent(s)
        # 1) Notify the batch container that it has a new child. Note that the parent jobs of
        # 'manual' batch jobs using the job_input.parent_job_id field *are not* modified to
        # include their children, so we don't do that here either.
        if batch_job:
            try:
                batch_job.modify(add_to_set__child_jobs=retry_job_id)
            except Exception as e:
                self._db_update_failure(
                    job_that_failed_operation=str(batch_job.id),
                    job_to_abort=retry_job_id,
                    exception=e,
                )

        # 2) Notify the retry_parent that it has been retried by adding a retry id
        try:
            job.modify(add_to_set__retry_ids=retry_job_id)
        except Exception as e:
            self._db_update_failure(
                job_that_failed_operation=str(job.id),
                job_to_abort=retry_job_id,
                exception=e,
            )
        # 3) If the retry_ids is updated and if present, the child_jobs, is updated, set toggle to true
        try:
            retry_job = self.sdkmr.get_mongo_util().get_job(job_id=retry_job_id)
            retry_job.modify(set__retry_saved_toggle=True)
        except Exception:
            self.logger.error(
                f"Couldn't toggle job retry state for {retry_job_id} ",
                exc_info=True,
                stack_info=True,
            )

        # Should we compare the original and child job to make sure certain fields match,
        # to make sure the retried job is correctly submitted? Or save that for a unit test?
        return {"job_id": job_id, "retry_id": retry_job_id}

    def retry(self, job_id: str, as_admin=False) -> Dict[str, Optional[str]]:
        """
        #TODO Add new job requirements/cgroups as an optional param
        :param job_id: The main job to retry
        :param as_admin: Run with admin permission
        :return: The child job id that has been retried
        """
        job, batch_job = self._validate_retry_presubmit(
            job_id=job_id, as_admin=as_admin
        )
        return self._retry(
            job_id=job_id, job=job, batch_job=batch_job, as_admin=as_admin
        )

    def retry_multiple(
        self, job_ids, as_admin=False
    ) -> List[Dict[str, Union[str, Any]]]:
        """
        #TODO Add new job requirements/cgroups as an optional param
        #TODO Notify the parent container that it has multiple new children, instead of multiple transactions?
        #TODO Prevent retry when multiple batch job containers?

        :param job_ids: The list of jobs to retry
        :param as_admin: Run with admin permission
        :return: The child job ids that have been retried or errors
        """
        if not job_ids:
            raise ValueError("No job_ids provided to retry")

        offending_ids = [item for item, count in Counter(job_ids).items() if count > 1]
        if offending_ids:
            raise ValueError(
                f"Retry of the same id in the same request is not supported."
                f" Offending ids: {offending_ids} "
            )

        # Check all inputs before attempting to start submitting jobs
        retried_jobs = []
        for job_id in job_ids:
            # Check for presubmission failures
            try:
                job, batch_job = self._validate_retry_presubmit(
                    job_id=job_id, as_admin=as_admin
                )
            except Exception as e:
                # Collect the presubmit error and don't submit the job
                retried_jobs.append({"job_id": job_id, "error": f"{e}"})
                continue
            # Presubmit worked, write to the db and submit
            try:
                retried_jobs.append(
                    self._retry(
                        job_id=job_id,
                        job=job,
                        batch_job=batch_job,
                        as_admin=as_admin,
                    )
                )
            except Exception as e:
                retried_jobs.append({"job_id": job_id, "error": f"{e}"})

        return retried_jobs

    @staticmethod
    def _get_run_job_params_from_existing_job(job: Job, user_id: str) -> Dict:
        """
        Get top level fields from job model to be sent into `run_job`
        """
        ji = job.job_input  # type: JobInput

        meta = None
        if ji.narrative_cell_info:
            meta = ji.narrative_cell_info.to_mongo().to_dict()

        source_ws_objects = list()
        if ji.source_ws_objects:
            source_ws_objects = list(ji.source_ws_objects)

        run_job_params = {
            _WORKSPACE_ID: job.wsid,
            _META: meta,
            _APP_PARAMS: ji.params or {},
            "user": user_id,  # REQUIRED, it runs as the current user
            _METHOD: ji.method,  # REQUIRED
            _APP_ID: ji.app_id,
            _SOURCE_WS_OBJECTS: source_ws_objects,  # Must be list
            _SERVICE_VER: ji.service_ver,
            _PARENT_JOB_ID: ji.parent_job_id,
            _BATCH_ID: job.batch_id,
        }

        # Then the next fields are job inputs top level requirements, app run parameters, and scheduler resource requirements
        return run_job_params

    def _check_ws_perms(
        self,
        runjob_params: Union[dict, list],
        new_batch_job: bool,
        batch_params: dict,
        as_admin: bool = False,
    ):
        """
        Check a single job, a single batch job, or a retry_multiple request with a mix of different jobs.
        """
        if as_admin:
            return self.sdkmr.check_as_admin(requested_perm=JobPermissions.WRITE)
        # Batch Param runs
        if new_batch_job:
            if batch_params:
                return self._check_workspace_permissions(batch_params.get("wsid"))
        # Single job runs
        elif isinstance(runjob_params, dict):
            return self._check_workspace_permissions(runjob_params.get("wsid"))
        # Multiple independent job runs, think retry_multiple()
        elif isinstance(runjob_params, list):
            return self._check_workspace_permissions_list(
                [job_param.get("wsid") for job_param in runjob_params]
            )
        else:
            raise IncorrectParamsException(
                "Runjob params must be an instance of a dict, or a list of dicts"
            )

    @staticmethod
    def _propagate_wsid_for_new_batch_jobs(
        runjob_params: dict, batch_params: dict, new_batch_job: bool
    ):
        """
        For batch jobs, check to make sure the job params do not provide a wsid other than None
        Then Modify the run job params to use the batch params wsid, which may be set to None
        """
        if new_batch_job:
            batch_wsid = batch_params.get("wsid") if batch_params else None
            for runjob_param in runjob_params:
                if runjob_param.get("wsid") is not None:
                    raise InvalidParameterForBatch()
                # Do we do a deepcopy here in case the params point to the same obj?
                runjob_param["wsid"] = batch_wsid

    def _preflight(
        self,
        runjob_params: Union[dict, list],
        batch_params: dict = None,
        new_batch_job: bool = False,
        as_admin: bool = False,
    ) -> None:
        """
        Propagate and check ws permissions for job(s)
        :param runjob_params: List of RunJobParams or a single RunJobParams mapping
        :param batch_params: Optional mapping for Batch Jobs
        :param new_batch_job: Whether or not this is a new batch job
        :param as_admin: For checking ws permissions as an admin or not
        """
        if batch_params and not new_batch_job:
            raise IncorrectParamsException(
                "Programming error, you forgot to set the new_batch_job flag to True"
            )
        if batch_params == runjob_params:
            raise IncorrectParamsException(
                "RunJobParams and BatchParams cannot be identical"
            )

        self._propagate_wsid_for_new_batch_jobs(
            runjob_params=runjob_params,
            batch_params=batch_params,
            new_batch_job=new_batch_job,
        )
        self._check_ws_perms(
            runjob_params=runjob_params,
            new_batch_job=new_batch_job,
            batch_params=batch_params,
            as_admin=as_admin,
        )

    def run(
        self, params=None, as_admin=False, concierge_params: Dict = None
    ) -> Optional[str]:
        """
        Warning: modifies params in place
        :param params: RunJobParams object (See spec file)
        :param as_admin: Allows you to run jobs in other people's workspaces
        :param concierge_params: Allows you to specify request_cpu, request_memory, request_disk, clientgroup
        :return: The condor job id
        """

        # TODO Test this
        if not isinstance(params, dict):
            raise IncorrectParamsException("params must be a mapping")

        self._preflight(runjob_params=params, as_admin=as_admin)

        if concierge_params:
            self.sdkmr.check_as_concierge()
            # we don't check requirements type because the concierge can do what they like
            params[_JOB_REQUIREMENTS] = self._get_job_reqs_from_concierge_params(
                params.get(_METHOD), concierge_params
            )
        else:
            # as_admin checked above
            self._add_job_requirements([params], bool(as_admin))
        self._check_job_arguments([params])
        return self._run(params=params)

    def _get_job_reqs_from_concierge_params(
        self, method: str, concierge_params: Dict[str, Any]
    ) -> ResolvedRequirements:
        jrr = self.sdkmr.get_job_requirements_resolver()
        norm = jrr.normalize_job_reqs(concierge_params, "concierge parameters")
        rl = concierge_params.get(_REQUIREMENTS_LIST)
        schd_reqs = {}
        if rl:
            if not isinstance(rl, list):
                raise IncorrectParamsException(f"{_REQUIREMENTS_LIST} must be a list")
            for s in rl:
                if not isinstance(s, str) or "=" not in s:
                    raise IncorrectParamsException(
                        f"Found illegal requirement in {_REQUIREMENTS_LIST}: {s}"
                    )
                key, val = s.split("=")
                schd_reqs[key.strip()] = val.strip()

        return jrr.resolve_requirements(
            method=method,
            catalog_cache=self.sdkmr.get_catalog_cache(),
            cpus=norm.get(REQUEST_CPUS),
            memory_MB=norm.get(REQUEST_MEMORY),
            disk_GB=norm.get(REQUEST_DISK),
            client_group=norm.get(CLIENT_GROUP) or CONCIERGE_CLIENTGROUP,
            client_group_regex=norm.get(CLIENT_GROUP_REGEX),
            # error messaging here is for 'bill_to_user' vs 'account_group' but almost impossible
            # to screw up so YAGNI
            # Note that this is never confirmed to be a real user. May want to fix that, but
            # since it's admin only... YAGNI
            bill_to_user=concierge_params.get("account_group"),
            # default is to ignore concurrency limits for concierge
            ignore_concurrency_limits=bool(
                concierge_params.get(IGNORE_CONCURRENCY_LIMITS, 1)
            ),
            scheduler_requirements=schd_reqs,
            debug_mode=norm.get(DEBUG_MODE),
        )

    def update_job_to_queued(self, job_id, scheduler_id):
        # TODO RETRY FOR RACE CONDITION OF RUN/CANCEL
        # TODO PASS QUEUE TIME IN FROM SCHEDULER ITSELF?
        # TODO PASS IN SCHEDULER TYPE?
        j = self.sdkmr.get_mongo_util().get_job(job_id=job_id)
        previous_status = j.status
        j.status = Status.queued.value
        j.queued = time.time()
        j.scheduler_id = scheduler_id
        j.scheduler_type = "condor"
        self.sdkmr.save_job(j)

        self.sdkmr.get_kafka_client().send_kafka_message(
            message=KafkaQueueChange(
                job_id=str(j.id),
                new_status=j.status,
                previous_status=previous_status,
                scheduler_id=scheduler_id,
            )
        )

    def get_job_params(self, job_id, as_admin=False):
        """
        get_job_params: fetch SDK method params passed to job runner
        Parameters:
        job_id: id of job
        Returns:
        job_params:
        """
        job_params = dict()
        job = self.sdkmr.get_job_with_permission(
            job_id, JobPermissions.READ, as_admin=as_admin
        )

        job_input = job.job_input

        job_params[_METHOD] = job_input.method
        job_params["params"] = job_input.params
        job_params["service_ver"] = job_input.service_ver
        job_params[_APP_ID] = job_input.app_id
        job_params[_WORKSPACE_ID] = job_input.wsid
        # This is specfically the data in the job params, which includes any manually submitted
        # parent job information but does not include batch job information
        job_params[_PARENT_JOB_ID] = job_input.parent_job_id
        job_params[_SOURCE_WS_OBJECTS] = job_input.source_ws_objects

        return job_params
