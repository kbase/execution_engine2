# -*- coding: utf-8 -*-
import unittest
import os
from configparser import ConfigParser
from datetime import datetime
from bson.objectid import ObjectId

from test.mongo_test_helper import MongoTestHelper
from execution_engine2.utils.MongoUtil import MongoUtil
from execution_engine2.models.models import Job


class MongoUtilTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        config_file = os.environ.get(
            "KB_DEPLOYMENT_CONFIG", os.path.join("test", "deploy.cfg")
        )
        print("Reading from" + config_file)
        config_parser = ConfigParser()
        config_parser.read(config_file)


        cls.config = {}
        for nameval in config_parser.items("execution_engine2"):
            cls.config[nameval[0]] = nameval[1]

        cls.config["mongo-collection"] = "jobs"

        cls.mongo_helper = MongoTestHelper()
        cls.test_collection = cls.mongo_helper.create_test_db(
            db=cls.config["mongo-database"], col=cls.config["mongo-collection"]
        )
        cls.mongo_util = MongoUtil(cls.config)

    @classmethod
    def tearDownClass(cls):
        print("Finished testing MongoUtil")

    def getMongoUtil(self):
        return self.__class__.mongo_util

    def test_init_ok(self):
        class_attri = [
            "mongo_host",
            "mongo_port",
            "mongo_database",
            "mongo_collection",
            "mongo_user",
            "mongo_pass",
            "mongo_authmechanism",
        ]
        mongo_util = self.getMongoUtil()
        self.assertTrue(set(class_attri) <= set(mongo_util.__dict__.keys()))

    def test_connection_ok(self):

        job = Job()

        user = "tgu2"
        job.user = user
        job.authstrat = "kbaseworkspace"
        job.wsid = 9999
        job.creation_time = datetime.timestamp(job.created)

        self.assertEqual(self.test_collection.count(), 0)
        with self.getMongoUtil().me_collection():
            job.save()
        self.assertEqual(self.test_collection.count(), 1)

        result = list(self.test_collection.find({"_id": job.id}))[0]

        expected_keys = [
            "_id",
            "user",
            "authstrat",
            "wsid",
            "created",
            "updated",
            "creation_time",
            "complete",
            "error",
        ]
        self.assertCountEqual(result.keys(), expected_keys)
        self.assertEqual(result["user"], user)
        self.assertEqual(result["authstrat"], "kbaseworkspace")
        self.assertEqual(result["wsid"], 9999)
        self.assertFalse(result["complete"])
        self.assertFalse(result["error"])

        self.assertFalse(result.get("job_input"))
        self.assertFalse(result.get("job_output"))

        self.test_collection.delete_one({"_id": job.id})
        self.assertEqual(self.test_collection.count(), 0)

    def test_insert_one_ok(self):

        mongo_util = self.getMongoUtil()

        with mongo_util.me_collection() as (pymongo_client, mongoengine_client):
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-collection"]
            ]

            self.assertEqual(col.count(), 0)
            doc = {"test_key": "foo"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(col.count(), 1)

            result = list(col.find({"_id": ObjectId(job_id)}))[0]
            self.assertEqual(result["test_key"], "foo")

            col.delete_one({"_id": ObjectId(job_id)})
            self.assertEqual(col.count(), 0)

    def test_find_in_ok(self):

        mongo_util = self.getMongoUtil()

        with mongo_util.me_collection() as (pymongo_client, mongoengine_client):
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-collection"]
            ]

            self.assertEqual(col.count(), 0)
            doc = {"test_key_1": "foo", "test_key_2": "bar"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(col.count(), 1)

            # test query empty field
            elements = ["foobar"]
            docs = mongo_util.find_in(elements, "test_key_1")
            self.assertEqual(docs.count(), 0)

            # test query "foo"
            elements = ["foo"]
            docs = mongo_util.find_in(elements, "test_key_1")
            self.assertEqual(docs.count(), 1)
            doc = docs.next()
            self.assertTrue("_id" in doc.keys())
            self.assertTrue(doc.get("_id"), job_id)
            self.assertEqual(doc.get("test_key_1"), "foo")

            col.delete_one({"_id": ObjectId(job_id)})
            self.assertEqual(col.count(), 0)

    def test_update_one_ok(self):

        mongo_util = self.getMongoUtil()

        with mongo_util.me_collection() as (pymongo_client, mongoengine_client):
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-collection"]
            ]

            self.assertEqual(col.count(), 0)
            doc = {"test_key_1": "foo"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(col.count(), 1)

            elements = ["foo"]
            docs = mongo_util.find_in(elements, "test_key_1")
            self.assertEqual(docs.count(), 1)
            doc = docs.next()
            self.assertTrue("_id" in doc.keys())
            self.assertTrue(doc.get("_id"), job_id)
            self.assertEqual(doc.get("test_key_1"), "foo")

            mongo_util.update_one({"test_key_1": "bar"}, job_id)

            elements = ["foo"]
            docs = mongo_util.find_in(elements, "test_key_1")
            self.assertEqual(docs.count(), 0)

            elements = ["bar"]
            docs = mongo_util.find_in(elements, "test_key_1")
            self.assertEqual(docs.count(), 1)

            col.delete_one({"_id": ObjectId(job_id)})
            self.assertEqual(col.count(), 0)

    def test_delete_one_ok(self):

        mongo_util = self.getMongoUtil()

        with mongo_util.me_collection() as (pymongo_client, mongoengine_client):
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-collection"]
            ]

            self.assertEqual(col.count(), 0)
            doc = {"test_key_1": "foo", "test_key_2": "bar"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(col.count(), 1)

            mongo_util.delete_one(job_id)
            self.assertEqual(col.count(), 0)
