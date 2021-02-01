# -*- coding: utf-8 -*-
import logging
import os
import unittest
from pprint import pprint

from pymongo import MongoClient
from pymongo.errors import OperationFailure

from test.utils_shared.test_utils import read_config_into_dict, bootstrap

logging.basicConfig(level=logging.INFO)
bootstrap()


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy = os.environ.get("KB_DEPLOYMENT_CONFIG", "/kb/module/deploy.cfg")
        config = read_config_into_dict(deploy, "execution_engine2")
        cls.config = config
        cls.ctx = {"job_id": "test", "user_id": "test", "token": "test"}
        cls.mongo_client = MongoClient(
            host=cls.config["mongo-host"],
            port=int(cls.config["mongo-port"]),
            username=cls.config["mongo-user"],
            password=cls.config["mongo-password"],
            authSource="admin",
            authMechanism=cls.config["mongo-authmechanism"],
            serverSelectionTimeoutMS=1000,
        )

        cls.db = cls.mongo_client.get_database(cls.config["mongo-database"])

        logging.info(f"Dropping user {cls.config['mongo-user']}")
        print(f"Dropping user {cls.config['mongo-user']}")
        try:
            cls.db.command("dropUser", cls.config["mongo-user"])
        except OperationFailure as e:
            logging.info("Couldn't drop user")
            print("Couldnt drop user", e)
            pprint(cls.config)
            logging.info(e)

        # TODO ADD USER TO EE2?

        try:
            cls.db.command(
                "createUser",
                cls.config["mongo-user"],
                pwd=cls.config["mongo-password"],
                roles=["dbOwner"],
            )
        except OperationFailure:
            logging.info("Couldn't add user")
            print("Couldn't add user")

        logging.info("Done running mongo setup")
        print("Done running mongo")

    def test_database_configured(self):
        logging.info("\nChecking privileged user exists")
        users_info = self.db.command("usersInfo")
        success = 0
        for user in users_info["users"]:
            if user["user"] == "travis":
                self.assertEqual(
                    [{"role": "dbOwner", "db": self.config["mongo-database"]}],
                    user["roles"],
                )
                success = 1
        self.assertTrue(success)
