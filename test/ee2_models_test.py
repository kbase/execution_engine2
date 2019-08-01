# -*- coding: utf-8 -*-
import logging
import unittest

logging.basicConfig(level=logging.INFO)

from configparser import ConfigParser

from execution_engine2.models.models import JobInput, Job, Meta, LogLines, JobLog

from pymongo import MongoClient
from bson import ObjectId
from mongoengine import connect


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config_parser = ConfigParser()
        cls.config_parser.read("deploy.cfg")
        cls.config = {}
        cls.config["mongo-host"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-host"
        )
        cls.config["mongo-database"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-database"
        )
        cls.config["mongo-user"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-user"
        )
        cls.config["mongo-password"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-password"
        )

        cls.config["mongo-collection"] = None
        cls.config["mongo-port"] = 27017
        cls.config["mongo-authmechanism"] = "DEFAULT"
        cls.config["start-local-mongo"] = 0

        cls.config["catalog-url"] = cls.config_parser.get(
            section="execution_engine2", option="catalog-url"
        )

        cls.config["workspace-url"] = cls.config_parser.get(
            section="execution_engine2", option="workspace-url"
        )

        cls.ctx = {"job_id": "test", "user_id": "test", "token": "test"}

        # You can set this up in the docker-compose
        cls.ee2_database = MongoClient(connectTimeoutMS=1000, host='mongo',
                                       port=cls.config['mongo-port']).get_database(
        cls.config['mongo-database'])
        cls.ee2_mongoengine_connect = connect('ee2', host='mongo', port=)

        cls.job_id = None

    def get_example_job(self):
        j = Job()
        j.user = 'boris'
        j.wsid = 123
        job_input = JobInput()
        job_input.wsid = j.wsid

        job_input.method = 'method'
        job_input.requested_release = 'requested_release'
        job_input.params = {}
        job_input.service_ver = 'dev'
        job_input.app_id = 'apple'

        m = Meta()
        m.cell_id = 'ApplePie'
        job_input.narrative_cell_info = m
        j.job_input = job_input

        j.status = 'queued'

        return j

    def test_insert_job(self):
        j = self.get_example_job()
        j.save()

        job = self.ee2_database.get_collection('job').find_one({"_id": ObjectId(j.id)})

        self.assertEqual(j.wsid, job['wsid'])
        self.assertEqual(j.job_input.narrative_cell_info.cell_id,
                         job['job_input']['narrative_cell_info']['cell_id'])

        self.job_id = j.id

        print("Job is is")
        print(self.job_id)

    def test_insert_log(self):
        job = self.get_example_job()
        job.save()

        j = JobLog()
        print(self.job_id )
        j.primary_key = job.id

        j.original_line_count = 1
        j.stored_line_count = 1
        j.lines = []
        j.lines.append(LogLines(error=True,linepos=0,line='The quick brown fox jumps over a lazy dog.'))

        j.save()

        #TODO Test adding lines to existing log, but we need the functions in ee2 first



