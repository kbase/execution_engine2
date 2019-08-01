# -*- coding: utf-8 -*-
import logging
import unittest

logging.basicConfig(level=logging.INFO)

from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.SDKMethodRunner import SDKMethodRunner
from configparser import ConfigParser
from pymongo import MongoClient, database


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config_parser = ConfigParser()
        cls.config_parser.read("deploy.cfg")
        cls.config = {}
        cls.config["mongo-host"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-host"
        )

        if cls.config_parser.get(section="execution_engine2", option='inside-docker-tests', fallback='1') == '1':
            cls.config['mongo-host'] = cls.config_parser.get('execution_engine2','inside-docker-mongo-host')

        cls.config["mongo-database"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-database"
        )
        cls.config["mongo-user"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-user"
        )
        cls.config["mongo-password"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-password"
        )

        cls.config["mongo-collection"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-collection"
        )
        cls.config["mongo-jobs-collection"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-jobs-collection"
        )
        cls.config["mongo-logs-collection"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-jobs-collection"
        )


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
        cls.sdkmr = SDKMethodRunner(cls.config)



        cls.mongo_client = MongoClient(
            host=cls.config['mongo-host'],
            port=cls.config['mongo-port'],
            username=cls.config['mongo-user'],
            password=cls.config['mongo-password'],
            authSource='admin',
            authMechanism=cls.config['mongo-authmechanism'],
        )

        logging.info("Creating privileged user")
        db = cls.mongo_client.get_database(cls.config['mongo-database'])
        db.command('dropUser', cls.config['mongo-user'])
        db.command("createUser", cls.config['mongo-user'], pwd=cls.config['mongo-password'], roles=["dbOwner"])



    def bootstrap(self):
        pass


    def test_create_job(self):
        pass

    def test_lookup_job(self):
        print("Begin test")
        print(self.mongo_client.list_database_names())
        self.sdkmr.run_job()