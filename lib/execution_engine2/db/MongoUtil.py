import logging
import subprocess
import traceback
from contextlib import contextmanager
import time

from bson.objectid import ObjectId
from mongoengine import connect, connection
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from execution_engine2.exceptions import (
    RecordNotFoundException,
    InvalidStatusTransitionException,
)
from execution_engine2.db.models.models import JobLog, Job, Status, TerminatedCode


class MongoUtil:
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
            logging.info("starting local mongod service")

            logging.info("running sudo service mongodb start")
            pipe = subprocess.Popen(
                "sudo service mongodb start",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout = pipe.communicate()
            logging.info(stdout)

            logging.info("running mongod --version")
            pipe = subprocess.Popen(
                "mongod --version",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            stdout = pipe.communicate()
            logging.info(stdout)

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
            logging.info(
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
            logging.info("no mongo-user found in config file, connecting without auth")
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

    def __init__(self, config: dict):
        self.config = config
        self.mongo_host = config["mongo-host"]
        self.mongo_port = int(config["mongo-port"])
        self.mongo_database = config["mongo-database"]
        self.mongo_user = config["mongo-user"]
        self.mongo_pass = config["mongo-password"]
        self.mongo_authmechanism = config["mongo-authmechanism"]

        self.mongo_collection = None

        self._start_local_service()
        logging.basicConfig(
            format="%(created)s %(levelname)s: %(message)s", level=logging.INFO
        )

    @contextmanager
    def pymongo_client(self, mongo_collection):
        """
        Instantiates a mongo client to be used as a context manager
        Closes the connection at the end
        :return:
        """
        self.mongo_collection = mongo_collection

        mc = MongoClient(
            self.mongo_host,
            self.mongo_port,
            username=self.mongo_user,
            password=self.mongo_pass,
            authSource=self.mongo_database,
            authMechanism=self.mongo_authmechanism,
        )

        try:
            yield mc
        finally:
            mc.close()

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

    def get_job(self, job_id=None, projection=None) -> Job:

        if job_id is None:
            raise ValueError("Please provide a job id")

        job = self.get_jobs(job_ids=[job_id], projection=projection)[0]

        return job

    def get_jobs(self, job_ids=None, projection=None, sort_id_ascending=None):
        if not (job_ids and isinstance(job_ids, list)):
            raise ValueError("Please provide a non empty list of job ids")

        if sort_id_ascending is None:
            sort_id_ascending = True

        sort_id_indicator = "+" if sort_id_ascending else "-"

        with self.mongo_engine_connection():
            try:
                if projection:
                    if not isinstance(projection, list):
                        raise ValueError("Please input a list type projection")
                    jobs = Job.objects(id__in=job_ids).exclude(*projection).order_by("{}_id".format(sort_id_indicator))
                else:
                    jobs = Job.objects(id__in=job_ids).order_by("{}_id".format(sort_id_indicator))
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
            raise InvalidStatusTransitionException(
                f"A job with status {job_status} cannot be terminated. It is already cancelled."
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
            j = self.get_job(job_id, projection=None)
            self.check_if_already_finished(j.status)
            if terminated_code is None:
                terminated_code = TerminatedCode.terminated_by_user.value
            j.finished = time.time()
            j.terminated_code = terminated_code
            j.status = Status.terminated.value
            j.save()

    def finish_job_with_error(self, job_id, error_message, error_code, error):
        """
        #TODO Should we check for a valid state transition here also?
        :param job_id:
        :param error_message:
        :param error_code:
        :return:
        """
        with self.mongo_engine_connection():
            j = self.get_job(job_id, projection=None)
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
            j = self.get_job(job_id, projection=None)
            j.job_output = job_output
            j.status = Status.completed.value
            j.finished = time.time()
            j.save()

    def update_job_status(self, job_id, status, msg=None, error_message=None):
        """
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

            logging.info(f"job status is {j.status}. going to update to {status}")

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
            j.save()

    def get_empty_job_log(self):
        jl = JobLog()
        jl.stored_line_count = 0
        jl.original_line_count = 0
        return jl

    @contextmanager
    def mongo_engine_connection(self):
        mongoengine_client = connect(
            db=self.mongo_database,
            host=self.mongo_host,
            port=self.mongo_port,
            username=self.mongo_user,
            password=self.mongo_pass,
            authentication_source=self.mongo_database,
            authentication_mechanism=self.mongo_authmechanism,
        )  # type: connection
        try:
            yield mongoengine_client
        finally:
            mongoengine_client.close()

    @contextmanager
    def me_collection(self, mongo_collection):
        self.mongo_collection = mongo_collection
        try:
            pymongo_client, mongoengine_client = self._get_collection(
                self.mongo_host,
                self.mongo_port,
                self.mongo_database,
                self.mongo_user,
                self.mongo_pass,
                self.mongo_authmechanism,
            )
            yield pymongo_client, mongoengine_client
        finally:
            pymongo_client.close()
            mongoengine_client.close()

    def insert_one(self, doc):
        """
        insert a doc into collection
        """
        logging.info("start inserting document")

        with self.me_collection(self.mongo_collection) as (
            pymongo_client,
            mongoengine_client,
        ):
            try:
                rec = pymongo_client[self.mongo_database][
                    self.mongo_collection
                ].insert_one(doc)
            except Exception as e:
                error_msg = "Connot insert doc\n"
                error_msg += "ERROR -- {}:\n{}".format(
                    e, "".join(traceback.format_exception(None, e, e.__traceback__))
                )
                raise ValueError(error_msg)

        return rec.inserted_id

    def update_one(self, doc, job_id):
        """
        update existing records
        https://docs.mongodb.com/manual/reference/operator/update/set/
        """
        logging.info("start updating document")

        with self.me_collection(self.mongo_collection) as (
            pymongo_client,
            mongoengine_client,
        ):
            job_col = pymongo_client[self.mongo_database][self.mongo_collection]
            try:
                update_filter = {"_id": ObjectId(job_id)}
                update = {"$set": doc}
                job_col.update_one(update_filter, update)
            except Exception as e:
                error_msg = "Connot update doc\n"
                error_msg += "ERROR -- {}:\n{}".format(
                    e, "".join(traceback.format_exception(None, e, e.__traceback__))
                )
                raise ValueError(error_msg)

        return True

    def delete_one(self, job_id):
        """
        delete a doc by _id
        """
        logging.info("start deleting document")
        with self.me_collection(self.mongo_collection) as (
            pymongo_client,
            mongoengine_client,
        ):
            job_col = pymongo_client[self.mongo_database][self.mongo_collection]
            try:
                delete_filter = {"_id": ObjectId(job_id)}
                job_col.delete_one(delete_filter)
            except Exception as e:
                error_msg = "Connot delete doc\n"
                error_msg += "ERROR -- {}:\n{}".format(
                    e, "".join(traceback.format_exception(None, e, e.__traceback__))
                )
                raise ValueError(error_msg)

        return True

    def find_in(self, elements, field_name, projection=None, batch_size=1000):
        """
        return cursor that contains docs which field column is in elements
        """
        logging.info("start querying MongoDB")

        with self.me_collection(self.mongo_collection) as (
            pymongo_client,
            mongoengine_client,
        ):
            job_col = pymongo_client[self.mongo_database][self.mongo_collection]
            try:
                result = job_col.find(
                    {field_name: {"$in": elements}},
                    projection=projection,
                    batch_size=batch_size,
                )
            except Exception as e:
                error_msg = "Connot query doc\n"
                error_msg += "ERROR -- {}:\n{}".format(
                    e, "".join(traceback.format_exception(None, e, e.__traceback__))
                )
                raise ValueError(error_msg)

            logging.info("returned {} results".format(result.count()))

        return result
