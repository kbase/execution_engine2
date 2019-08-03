# -*- coding: utf-8 -*-
import logging
import os
import unittest
from configparser import ConfigParser

from bson.objectid import ObjectId

from execution_engine2.models.models import Job, JobInput, Meta
from execution_engine2.utils.MongoUtil import MongoUtil
from test.mongo_test_helper import MongoTestHelper

logging.basicConfig(level=logging.INFO)
from test.test_utils import bootstrap

bootstrap()


class MongoUtilTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", os.path.join("test", "deploy.cfg"))

        logging.info("Reading from " + config_file)
        config_parser = ConfigParser()
        config_parser.read(config_file)

        cls.config = {}
        for nameval in config_parser.items("execution_engine2"):
            cls.config[nameval[0]] = nameval[1]

        cls.config["start-local-mongo"] = "1"

        logging.info("Setting up mongo test helper")
        cls.mongo_helper = MongoTestHelper(cls.config)
        logging.info(
            f" mongo create test db={cls.config['mongo-database']}, col={cls.config['mongo-jobs-collection']}"
        )

        cls.test_collection = cls.mongo_helper.create_test_db(
            db=cls.config["mongo-database"], col=cls.config["mongo-jobs-collection"]
        )
        logging.info("Setting up mongo util")
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
            "mongo_user",
            "mongo_pass",
            "mongo_authmechanism",
            "mongo_collection"
        ]
        mongo_util = self.getMongoUtil()
        self.assertTrue(set(class_attri) <= set(mongo_util.__dict__.keys()))

    def test_connection_ok(self):
        j = Job()
        user = "boris"
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

        self.assertEqual(self.test_collection.count_documents({}), 0)
        with self.getMongoUtil().me_collection(self.config["mongo-jobs-collection"]):
            j.save()

        print(self.test_collection.name)
        self.assertEqual(self.test_collection.count_documents({}), 1)

        result = list(self.test_collection.find({"_id": j.id}))[0]

        expected_keys = [
            "_id",
            "user",
            "authstrat",
            "wsid",
            "status",
            "updated",
            "job_input",
        ]

        meta = {"collection": "ee2_jobs"}

        self.assertCountEqual(result.keys(), expected_keys)
        self.assertEqual(result["user"], j.user)
        self.assertEqual(result["authstrat"], "kbaseworkspace")
        self.assertEqual(result["wsid"], j.wsid)

        # self.assertFalse(result["error"])
        # self.assertFalse(result.get("job_input"))
        # self.assertFalse(result.get("job_output"))

        self.test_collection.delete_one({"_id": j.id})
        self.assertEqual(self.test_collection.count_documents({}), 0)

    def test_insert_one_ok(self):
        mongo_util = self.getMongoUtil()

        with mongo_util.me_collection(self.config["mongo-jobs-collection"]) as (pymongo_client, mongoengine_client):
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-jobs-collection"]
            ]

            self.assertEqual(col.count_documents({}), 0)
            doc = {"test_key": "foo"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(col.count_documents({}), 1)

            result = list(col.find({"_id": ObjectId(job_id)}))[0]
            self.assertEqual(result["test_key"], "foo")

            col.delete_one({"_id": ObjectId(job_id)})
            self.assertEqual(col.count_documents({}), 0)

    def test_find_in_ok(self):
        mongo_util = self.getMongoUtil()

        with mongo_util.me_collection(self.config["mongo-jobs-collection"]) as (pymongo_client, mongoengine_client):
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-jobs-collection"]
            ]

            self.assertEqual(col.count_documents({}), 0)
            doc = {"test_key_1": "foo", "test_key_2": "bar"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(col.count_documents({}), 1)

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
            self.assertEqual(col.count_documents({}), 0)

    def test_update_one_ok(self):
        mongo_util = self.getMongoUtil()

        with mongo_util.me_collection(self.config["mongo-jobs-collection"]) as (pymongo_client, mongoengine_client):
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-jobs-collection"]
            ]

            self.assertEqual(col.count_documents({}), 0)
            doc = {"test_key_1": "foo"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(col.count_documents({}), 1)

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
            self.assertEqual(col.count_documents({}), 0)

    def test_delete_one_ok(self):
        mongo_util = MongoUtil(self.config)
        with mongo_util.pymongo_client(self.config["mongo-jobs-collection"]) as pc:
            col = pc.get_database(self.config["mongo-database"]).get_collection(
                self.config["mongo-jobs-collection"]
            )

            doc_count = col.count_documents({})
            logging.info("Found {} documents".format(doc_count))

            doc = {"test_key_1": "foo", "test_key_2": "bar"}
            job_id = mongo_util.insert_one(doc)

            self.assertEqual(col.count_documents({}), doc_count + 1)
            logging.info("Assert 0 documents")
            mongo_util.delete_one(job_id)
            self.assertEqual(col.count_documents({}), doc_count)
