import logging
import subprocess
import time
import traceback
from contextlib import contextmanager
from typing import Dict, List, NamedTuple
from bson.objectid import ObjectId
from mongoengine import connect, connection
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from execution_engine2.db.models.models import JobLog, Job, Status, TerminatedCode
from execution_engine2.exceptions import (
    RecordNotFoundException,
    InvalidStatusTransitionException,
)

from lib.execution_engine2.utils.arg_processing import parse_bool


class JobIdPair(NamedTuple):
    job_id: str
    scheduler_id: str


class MongoUtil:
    def __init__(self, config: Dict):
        self.config = config
        self.mongo_host = config["mongo-host"]
        self.mongo_port = int(config["mongo-port"])
        self.mongo_database = config["mongo-database"]
        self.mongo_user = config["mongo-user"]
        self.mongo_pass = config["mongo-password"]
        self.retry_rewrites = parse_bool(config["mongo-retry-rewrites"])
        self.mongo_authmechanism = config["mongo-authmechanism"]
        self._col_jobs = config["mongo-jobs-collection"]
        self._col_logs = config["mongo-logs-collection"]
        self._start_local_service()
        self.logger = logging.getLogger("ee2")
        self.pymongoc = self._get_pymongo_client()
        self.me_connection = self._get_mongoengine_client()

    def _get_pymongo_client(self):
        return MongoClient(
            self.mongo_host,
            self.mongo_port,
            username=self.mongo_user,
            password=self.mongo_pass,
            authSource=self.mongo_database,
            authMechanism=self.mongo_authmechanism,
            retryWrites=self.retry_rewrites,
        )

    def _get_mongoengine_client(self) -> connection:
        return connect(
            db=self.mongo_database,
            host=self.mongo_host,
            port=self.mongo_port,
            username=self.mongo_user,
            password=self.mongo_pass,
            authentication_source=self.mongo_database,
            authentication_mechanism=self.mongo_authmechanism,
            retryWrites=self.retry_rewrites,
        )
        # This MongoDB deployment does not support retryable writes

    def _start_local_service(self):
        try:
            start_local = int(self.config.get("start-local-mongo", 0))
        except Exception:
            raise ValueError(
                "unexpected start-local-mongo: {}".format(
                    self.config.get("start-local-mongo")
                )
            )
        if start_local:
            print("Start local is")
            print(start_local)
            self.logger.debug("starting local mongod service")

            self.logger.debug("running sudo service mongodb start")
            pipe = subprocess.Popen(
                "sudo service mongodb start",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout = pipe.communicate()
            self.logger.debug(stdout)

            self.logger.debug("running mongod --version")
            pipe = subprocess.Popen(
                "mongod --version",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            stdout = pipe.communicate()
            self.logger.debug(stdout)

    @classmethod
    def _get_collection(
        self,
        mongo_host: str,
        mongo_port: int,
        mongo_database: str,
        mongo_user: str = None,
        mongo_password: str = None,
        mongo_authmechanism: str = "DEFAULT",
    ):
        """
        Connect to Mongo server and return a tuple with the MongoClient and MongoClient?
        """

        if mongo_user:
            self.logger.debug(
                "mongo-user found in config file, configuring client for authentication using mech "
                + str(mongo_authmechanism)
            )
            pymongo_client = MongoClient(
                mongo_host,
                mongo_port,
                username=mongo_user,
                password=mongo_password,
                authSource=mongo_database,
                authMechanism=mongo_authmechanism,
            )

            mongoengine_client = connect(
                db=mongo_database,
                host=mongo_host,
                port=mongo_port,
                username=mongo_user,
                password=mongo_password,
                authentication_source=mongo_database,
                authentication_mechanism=mongo_authmechanism,
            )
        else:
            self.logger.debug(
                "no mongo-user found in config file, connecting without auth"
            )
            pymongo_client = MongoClient(mongo_host, mongo_port)

            mongoengine_client = connect(
                mongo_database, host=mongo_host, port=mongo_port
            )
        try:
            pymongo_client.server_info()  # force a call to server
        except ServerSelectionTimeoutError as e:
            error_msg = "Cannot connect to Mongo server\n"
            error_msg += "ERROR -- {}:\n{}".format(
                e, "".join(traceback.format_exception(None, e, e.__traceback__))
            )
            raise ValueError(error_msg)

        return pymongo_client, mongoengine_client

    def get_workspace_jobs(self, workspace_id):
        with self.mongo_engine_connection():
            job_ids = [str(job.id) for job in Job.objects(wsid=workspace_id)]
            return job_ids

    def get_job_log_pymongo(self, job_id: str = None):

        job_log_col = self.pymongoc[self.mongo_database][self._col_logs]
        try:
            find_filter = {"_id": ObjectId(job_id)}
            job_log = job_log_col.find_one(find_filter)
        except Exception as e:
            error_msg = "Unable to find job\n"
            error_msg += "ERROR -- {}:\n{}".format(
                e, "".join(traceback.format_exception(None, e, e.__traceback__))
            )
            raise ValueError(error_msg)

        if not job_log:
            raise RecordNotFoundException(
                "Cannot find job log with id: {}".format(job_id)
            )

        return job_log

    def get_job_log(self, job_id: str = None) -> JobLog:
        if job_id is None:
            raise ValueError("Please provide a job id")
        with self.mongo_engine_connection():
            try:
                job_log = JobLog.objects.with_id(job_id)
            except Exception:
                raise ValueError(
                    "Unable to find job:\nError:\n{}".format(traceback.format_exc())
                )

            if not job_log:
                raise RecordNotFoundException(
                    "Cannot find job log with id: {}".format(job_id)
                )

        return job_log

    def get_job(self, job_id=None, exclude_fields=None) -> Job:
        """
        TODO Do we really need to call get jobs here? Or should we make own function to make it faster
        :param job_id:
        :param exclude_fields:
        :return:
        """

        if job_id is None:
            raise ValueError("Please provide a valid job id")

        job = self.get_jobs(job_ids=[job_id], exclude_fields=exclude_fields)[0]

        return job

    def get_jobs(
        self, job_ids=None, exclude_fields=None, sort_id_ascending=None
    ) -> List[Job]:
        if not (job_ids and isinstance(job_ids, list)):
            raise ValueError("Please provide a non empty list of job ids")

        if sort_id_ascending is None:
            sort_id_ascending = True

        sort_id_indicator = "+" if sort_id_ascending else "-"

        with self.mongo_engine_connection():
            try:

                if exclude_fields:
                    if not isinstance(exclude_fields, list):
                        raise ValueError("Please input a list type exclude_fields")
                    jobs = (
                        Job.objects(id__in=job_ids)
                        .exclude(*exclude_fields)
                        .order_by("{}_id".format(sort_id_indicator))
                    )

                else:
                    jobs = Job.objects(id__in=job_ids).order_by(
                        "{}_id".format(sort_id_indicator)
                    )
            except Exception:
                raise ValueError(
                    "Unable to find job:\nError:\n{}".format(traceback.format_exc())
                )

            if not jobs:
                raise RecordNotFoundException(
                    "Cannot find job with ids: {}".format(job_ids)
                )

        return jobs

    @staticmethod
    def check_if_already_finished(job_status):
        if job_status in [
            Status.error.value,
            Status.completed.value,
            Status.terminated.value,
        ]:
            return True
        return False

    def update_job_to_queued(
        self, job_id: str, scheduler_id: str, scheduler_type: str = "condor"
    ) -> None:
        f"""
        * Updates a {Status.created.value} job to queued and sets scheduler state.
          Always sets scheduler state, but will only update to queued if the job is in the
          {Status.created.value} state.
        :param job_id: the ID of the job.
        :param scheduler_id: the scheduler's job ID for the job.
        :param scheduler_type: The scheduler this job was queued in, default condor
        """
        if not job_id or not scheduler_id or not scheduler_type:
            raise ValueError("None of the 3 arguments can be falsy")
        # could also test that the job ID is a valid job ID rather than having mongo throw an
        # error
        queue_time_now = time.time()
        ee2_jobs_col = self.pymongoc[self.mongo_database][self._col_jobs]
        # should we check that the job was updated and do something if it wasn't?
        ee2_jobs_col.update_one(
            {"_id": ObjectId(job_id), "status": Status.created.value},
            {"$set": {"status": Status.queued.value, "queued": queue_time_now}},
        )
        # originally had a single query, but seems safer to always record the scheduler
        # state no matter the state of the job
        ee2_jobs_col.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "scheduler_id": scheduler_id,
                    "scheduler_type": scheduler_type,
                }
            },
        )

    def cancel_job(self, job_id=None, terminated_code=None):
        """
        #TODO Should we check for a valid state transition here also?
        #TODO Make cancel code mandatory and part of spec?
        #TODO Should make terminated_code default to something else, and update clients in Narrative?
        :param job_id: Cancel job by id
        :param terminated_code: Default to terminated by user
        """

        with self.mongo_engine_connection():
            j = self.get_job(job_id)
            if self.check_if_already_finished(j.status):
                return False
            if terminated_code is None:
                terminated_code = TerminatedCode.terminated_by_user.value
            j.finished = time.time()
            j.terminated_code = terminated_code
            j.status = Status.terminated.value
            j.save()

        return True

    def finish_job_with_error(self, job_id, error_message, error_code, error):
        """
        #TODO Should we check for a valid state transition here also?
        :param error:
        :param job_id:
        :param error_message:
        :param error_code:
        :return:
        """
        with self.mongo_engine_connection():
            j = self.get_job(job_id)
            j.error_code = error_code
            j.errormsg = error_message
            j.error = error
            j.status = Status.error.value
            j.finished = time.time()
            j.save()

    def finish_job_with_success(self, job_id, job_output):
        """
        #TODO Should we check for a valid state transition here also?
        :param job_id:
        :param job_output:
        :param job:
        :return:
        """
        with self.mongo_engine_connection():
            j = self.get_job(job_id)
            j.job_output = job_output
            j.status = Status.completed.value
            j.finished = time.time()
            j.save()

    def get_job_batch_name(self, cluster_id):
        """
        Convert Condor ID into Job ID
        :param cluster_id: The condor ID
        :return:  The JobBatchName / EE2 Record ID
        """
        # TODO Create an index on this field?
        with self.mongo_engine_connection():
            j = Job.objects(scheduler_id=cluster_id)
            if len(j) == 0:
                raise RecordNotFoundException(f"Cluster id of {cluster_id}")
            return str(j[0].id)

    def update_job_resources(self, job_id, resources):
        """
        Save resources used by job, as reported by condor
        :param job_id: The job id to save resources for
        :param resources: The resources used by the job, as reported by condor
        :return:
        """
        self.logger.debug(f"About to add {resources} to {job_id}")
        with self.mongo_engine_connection():
            j = Job.objects.with_id(job_id)  # type: Job
            j.condor_job_ads = resources
            j.save()

    def update_job_status(self, job_id, status, msg=None, error_message=None):
        """
        #TODO Deprecate this function, and create a StartJob or StartEstimating Function

        A job in status created can be estimating/running/error/terminated
        A job in status created cannot be created

        A job in status estimating can be running/finished/error/terminated
        A job in status estimating cannot be created or estimating

        A job in status running can be terminated/error/finished
        A job in status running cannot be created/estimating

        A job in status finished/terminated/error cannot be changed

        """

        with self.mongo_engine_connection():
            j = Job.objects.with_id(job_id)  # type: Job
            #  A job in status finished/terminated/error cannot be changed
            if j.status in [
                Status.completed.value,
                Status.terminated.value,
                Status.error.value,
            ]:
                raise InvalidStatusTransitionException(
                    f"Cannot change already finished/terminated/errored job.  {j.status} to {status}"
                )

            #  A job in status running can only be terminated/error/finished
            if j.status == Status.running.value:
                if status not in [
                    Status.completed.value,
                    Status.terminated.value,
                    Status.error.value,
                ]:
                    raise InvalidStatusTransitionException(
                        f"Cannot change from {j.status} to {status}"
                    )

            # A job in status estimating cannot be created
            if j.status == Status.estimating.value:
                if status == Status.created.value:
                    raise InvalidStatusTransitionException(
                        f"Cannot change from {j.status} to {status}"
                    )

            # A job in status X cannot become status X
            if j.status == status:
                raise InvalidStatusTransitionException(
                    f"Cannot change from {j.status} to itself {status}"
                )

            if error_message and msg:
                raise Exception(
                    "You can't set both error and msg at the same time because of.. Reasons?"
                )

            if error_message:
                j.errormsg = error_message
            elif msg:
                j.msg = msg

            j.status = status

            if status == Status.running.value:
                j.running = time.time()
            elif status == Status.estimating.value:
                j.estimating = time.time()

            j.save()

    @contextmanager
    def mongo_engine_connection(self):
        yield self.me_connection

    def insert_jobs(self, jobs_to_insert: List[Job]) -> List[ObjectId]:
        """
        Insert multiple job records using MongoEngine
        :param jobs_to_insert: Multiple jobs to insert at once
        :return: List of job ids from the insertion
        """
        # TODO Look at pymongo write_concerns that may be useful
        # TODO see if pymongo is faster
        # TODO: Think about error handling
        inserted = Job.objects.insert(doc_or_docs=jobs_to_insert, load_bulk=False)
        return inserted

    def _push_job_logs(self, log_lines: JobLog, job_id: str, record_count: int):
        """append a list of job logs, and update the record count"""

        update_filter = {"_id": ObjectId(job_id)}
        push_op = {"lines": {"$each": log_lines}}

        set_op = {
            "original_line_count": record_count,
            "stored_line_count": record_count,
            "updated": time.time(),
        }
        update = {"$push": push_op, "$set": set_op}
        job_col = self.pymongoc[self.mongo_database][self._col_logs]
        try:
            job_col.update_one(update_filter, update, upsert=False)
        except Exception as e:
            error_msg = "Cannot update doc\n ERROR -- {}:\n{}".format(
                e, "".join(traceback.format_exception(None, e, e.__traceback__))
            )
            raise ValueError(error_msg)

        slc = job_col.find_one({"_id": ObjectId(job_id)}).get("stored_line_count")

        return slc
