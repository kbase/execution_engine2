#!/usr/bin/env python
# type: ignore
import os
import sys
from configparser import ConfigParser
from collections import Counter
from pymongo import MongoClient

jobs_database_name = "ee2_jobs"
from bson import ObjectId

try:
    from lib.execution_engine2.db.models.models import (
        Job,
        Status,
        ErrorCode,
        JobInput,
        Meta,
        TerminatedCode,
    )

except Exception:
    from models import Job, Status, ErrorCode, JobInput, Meta, TerminatedCode

UNKNOWN = "UNKNOWN"


class Lookup:
    """
    GET UJS Record, Get corresponding NJS record, combine the two and save them in a new
    collection, using the UJS ID as the primary key
    """

    documents = []
    threshold = 1000
    none_jobs = 0

    def _get_ee2_connection(self) -> MongoClient:
        parser = ConfigParser()
        parser.read(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        self.ee2_host = parser.get("execution_engine2", "mongo-host")
        self.ee2_db = "exec_engine2"
        self.ee2_user = parser.get("execution_engine2", "mongo-user")
        self.ee2_pwd = parser.get("execution_engine2", "mongo-password")

        return MongoClient(
            self.ee2_host,
            27017,
            username=self.ee2_user,
            password=self.ee2_pwd,
            authSource=self.ee2_db,
            retryWrites=False,
        )

    def _get_ujs_connection(self) -> MongoClient:
        parser = ConfigParser()
        parser.read(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        self.ujs_host = parser.get("NarrativeJobService", "ujs-mongodb-host")
        self.ujs_db = parser.get("NarrativeJobService", "ujs-mongodb-database")
        self.ujs_user = parser.get("NarrativeJobService", "ujs-mongodb-user")
        self.ujs_pwd = parser.get("NarrativeJobService", "ujs-mongodb-pwd")
        self.ujs_jobs_collection = "jobstate"
        return MongoClient(
            self.ujs_host,
            27017,
            username=self.ujs_user,
            password=self.ujs_pwd,
            authSource=self.ujs_db,
        )

    def _get_njs_connection(self) -> MongoClient:
        parser = ConfigParser()
        parser.read(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        self.njs_host = parser.get("NarrativeJobService", "mongodb-host")
        self.njs_db = parser.get("NarrativeJobService", "mongodb-database")
        self.njs_user = parser.get("NarrativeJobService", "mongodb-user")
        self.njs_pwd = parser.get("NarrativeJobService", "mongodb-pwd")
        self.njs_jobs_collection_name = "exec_tasks"
        self.njs_logs_collection_name = "exec_logs"

        return MongoClient(
            self.njs_host,
            27017,
            username=self.njs_user,
            password=self.njs_pwd,
            authSource=self.njs_db,
            retryWrites=False,
        )

    def __init__(self):
        # Use this after adding more config variables
        self.ee2 = self._get_ee2_connection()
        self.njs = self._get_njs_connection()
        self.ujs = self._get_ujs_connection()
        self.jobs = []
        self.threshold = 1000

        self.ujs_jobs = (
            self._get_ujs_connection()
            .get_database(self.ujs_db)
            .get_collection(self.ujs_jobs_collection)
        )
        self.njs_jobs = (
            self._get_njs_connection()
            .get_database(self.njs_db)
            .get_collection(self.njs_jobs_collection_name)
        )

        self.ee2_jobs = (
            self._get_ee2_connection()
            .get_database(self.ee2_db)
            .get_collection(jobs_database_name)
        )

    def get_njs_job_input(self, njs_job):
        job_input = njs_job.get("job_input")
        if job_input is None:
            self.none_jobs += 1
            print(
                "Found ujs job with corresponding njs job with no job input ",
                self.none_jobs,
                njs_job["ujs_job_id"],
            )
            job_input = {
                "service_ver": UNKNOWN,
                "method": UNKNOWN,
                "app_id": UNKNOWN,
                "params": UNKNOWN,
                "wsid": -1,
            }

        if type(job_input) is list:
            return job_input[0]

        return job_input

    def save_job(self, job):
        try:
            self.ee2_jobs.insert_one(document=job.to_mongo())
        except Exception as e:
            print(e)

    def save_remnants(self):
        self.ee2_jobs.insert_many(self.jobs)
        self.jobs = []

    # flake8: noqa: C901
    def begin_job_transfer(self):  # flake8: noqa
        ujs_jobs = self.ujs_jobs
        njs_jobs = self.njs_jobs
        ee2_jobs = self.ee2_jobs

        # job_id = sys.argv[1]
        # ujs = ujs_jobs.find_one({"_id": {"$eq": ObjectId(job_id)}})
        # njs = njs_jobs.find_one({"ujs_job_id": {"$eq": job_id}})
        # njs_fake = njs_jobs.find_one({"ujs_job_id": {"$eq": job_id}})
        # ee2 = ee2_jobs.find_one({"_id": {"$eq": ObjectId(job_id)}})
        #
        # print("ujs", ujs)
        # print("njs", njs)
        # print("njs_fake", njs_fake)
        # print("ee2", ee2)

        ee2_missing = ee2_jobs.find({"job_input": {"$eq": None}})
        status = Counter()
        for job in ee2_missing:
            status[job.get("status")] += 1
            print(job["_id"], job.get("status"), job["_id"].generation_time)

        print("Status is")
        print(status)


c = Lookup()
c.begin_job_transfer()
