# -*- coding: utf-8 -*-
import logging
import unittest

logging.basicConfig(level=logging.INFO)

from configparser import ConfigParser

from execution_engine2.models.models import JobInput, Job, Meta, LogLines, JobLog
from execution_engine2.utils.MongoUtil import MongoUtil
from test.test_utils import read_config_into_dict

from bson import ObjectId


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = read_config_into_dict()
        # Should this just be added into read_config_into_dict function?
        mongo_in_docker = config.get('mongo-in-docker-compose', None)
        if mongo_in_docker is not None:
            config['mongo-host'] = config['mongo-in-docker-compose']

        # For using mongo running in docker
        config['start-local-mongo'] = 0

        cls.config = config
        cls.ctx = {"job_id": "test", "user_id": "test", "token": "test"}
        cls.mongo_util = MongoUtil(cls.config)


    def get_example_job(self):
        j = Job()
        j.user = "boris"
        j.wsid = 123
        job_input = JobInput()
        job_input.wsid = j.wsid

        job_input.method = "method"
        job_input.requested_release = "requested_release"
        job_input.params = {}
        job_input.service_ver = "dev"
        job_input.app_id = "apple"

        m = Meta()
        m.cell_id = "ApplePie"
        job_input.narrative_cell_info = m
        j.job_input = job_input
        j.status = "queued"

        return j

    def test_insert_job(self):
        logging.info("Testing insert job")
        with self.mongo_util.mongo_engine_connection(), self.mongo_util.pymongo_client() as pc:

            job = self.get_example_job()
            job.save()

            logging.info(f"Inserted {job.id}")

            logging.info(f"Searching for {job.id}")
            db = self.config["mongo-database"]
            coll = self.config['mongo-jobs-collection']
            saved_job = pc[db][coll].find_one({"_id": ObjectId(job.id)})
            logging.info("Found")
            logging.info(saved_job)

            print(job.wsid)
            print(saved_job['wsid'])
            self.assertEqual(job.wsid, saved_job["wsid"])
            self.assertEqual(
                job.job_input.narrative_cell_info.cell_id,
                saved_job["job_input"]["narrative_cell_info"]["cell_id"],
            )


    def test_insert_log(self):
        with self.mongo_util.mongo_engine_connection():
            job = self.get_example_job()
            job.save()

            j = JobLog()
            print(job.id)
            j.primary_key = job.id

            j.original_line_count = 1
            j.stored_line_count = 1
            j.lines = []
            j.lines.append(
                LogLines(
                    error=True,
                    linepos=0,
                    line="The quick brown fox jumps over a lazy dog.",
                )
            )

            j.save()

        # TODO Test adding lines to existing log, but we need the functions in ee2 first
