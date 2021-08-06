# -*- coding: utf-8 -*-
import logging
import os
import unittest
from datetime import datetime

from bson.objectid import ObjectId

from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.db.models.models import Job, JobLog, Status
from execution_engine2.sdk.EE2Runjob import JobIdPair
from test.utils_shared.test_utils import (
    bootstrap,
    get_example_job,
    read_config_into_dict,
)
from tests_for_db.mongo_test_helper import MongoTestHelper

logging.basicConfig(level=logging.INFO)

bootstrap()


class MongoUtilTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
        config = read_config_into_dict(deploy)
        cls.config = config

        logging.info("Setting up mongo test helper")
        cls.mongo_helper = MongoTestHelper(cls.config)
        logging.info(
            f" mongo create test db={cls.config['mongo-database']}, col={cls.config['mongo-jobs-collection']}"
        )

        cls.test_collection = cls.mongo_helper.create_test_db(db="ee2", col="ee2_jobs")
        logging.info("Setting up mongo util")
        cls.mongo_util = MongoUtil(cls.config)

    @classmethod
    def tearDownClass(cls):
        print("Finished testing MongoUtil")

    def getMongoUtil(self) -> MongoUtil:
        return self.__class__.mongo_util

    def test_init_ok(self):
        class_attri = [
            "mongo_host",
            "mongo_port",
            "mongo_database",
            "mongo_user",
            "mongo_pass",
            "mongo_authmechanism",
            "mongo_collection",
        ]
        mongo_util = self.getMongoUtil()
        self.assertTrue(set(class_attri) <= set(mongo_util.__dict__.keys()))

    def test_insert_jobs(self):
        """Check to see that jobs are inserted into mongo"""
        job = get_example_job(status=Status.created.value)
        job2 = get_example_job(status=Status.created.value)
        jobs_to_insert = [job, job2]
        job_ids = self.getMongoUtil().insert_jobs(jobs_to_insert)
        assert len(job_ids) == len(jobs_to_insert)
        retrieved_jobs = self.getMongoUtil().get_jobs(job_ids=job_ids)

        for i, retrieved_job in enumerate(retrieved_jobs):
            assert jobs_to_insert[i].to_json() == retrieved_job.to_json()

    def test_update_jobs_enmasse(self):
        """Check to see that created jobs get updated to queued"""
        for state in Status:
            job = get_example_job(status=Status.created.value, scheduler_id=None)
            job2 = get_example_job(status=state.value, scheduler_id=None)
            job3 = get_example_job(status=state.value, scheduler_id=None)
            jobs = [job, job2, job3]

            for j in jobs:
                j.scheduler_id = None
                j.save()
                assert j.scheduler_id is None

            job_ids = [job.id, job2.id, job3.id]
            scheduler_ids = ["humpty", "dumpty", "alice"]
            jobs_to_update = list(map(JobIdPair, job_ids, scheduler_ids))

            now_ms = datetime.utcnow().timestamp()

            self.getMongoUtil().update_jobs_to_queued(jobs_to_update)
            job.reload()
            job2.reload()
            job3.reload()

            # Check that sched ids are set
            for i, val in enumerate(scheduler_ids):
                assert jobs[i].scheduler_id == val
                assert jobs[i].scheduler_type == "condor"

            #  Checks that a timestamp in seconds since the epoch is within a second of the current time.
            for j in jobs:
                assert now_ms + 1 > j.updated
                assert now_ms - 1 < j.updated

        # First job always should transition to queued
        assert job.status == Status.queued.value

        # Created jobs should transition
        if state.value == Status.created.value:
            assert all(j.status == Status.queued.value for j in [job, job2, job3])

        else:
            # Don't change their state
            assert all(j.status == state.value for j in [job2, job3])

    def test_update_jobs_enmasse_bad_job_pairs(self):
        job = get_example_job(status=Status.created.value).save()
        job2 = get_example_job(status=Status.created.value).save()
        job3 = get_example_job(status=Status.created.value).save()
        job_ids = [job.id, job2.id, job3.id]
        scheduler_ids = [job.scheduler_id, job2.scheduler_id, None]
        job_id_pairs = list(map(JobIdPair, job_ids, scheduler_ids))

        with self.assertRaisesRegex(
            expected_exception=ValueError,
            expected_regex=f"Provided a bad job_id_pair, missing scheduler_id for {job3.id}",
        ):
            self.getMongoUtil().update_jobs_to_queued(job_id_pairs)

        job_ids = [job.id, job2.id, None]
        scheduler_ids = [job.scheduler_id, job2.scheduler_id, job3.scheduler_id]
        job_id_pairs = list(map(JobIdPair, job_ids, scheduler_ids))

        with self.assertRaisesRegex(
            expected_exception=ValueError,
            expected_regex=f"Provided a bad job_id_pair, missing job_id for {job3.scheduler_id}",
        ):
            self.getMongoUtil().update_jobs_to_queued(job_id_pairs)

    def test_get_by_cluster(self):
        """Get a job by its condor scheduler_id"""
        mongo_util = self.getMongoUtil()
        with mongo_util.mongo_engine_connection():
            job = get_example_job()
            job_id = job.save().id
            batch = mongo_util.get_job_batch_name(job.scheduler_id)
            self.assertEqual(str(job_id), batch)

    def test_get_job_ok(self):
        mongo_util = self.getMongoUtil()

        with mongo_util.mongo_engine_connection():
            ori_job_count = Job.objects.count()
            job = get_example_job()
            job_id = job.save().id
            self.assertEqual(ori_job_count, Job.objects.count() - 1)

            # get job with no exclude_fields
            job = mongo_util.get_job(job_id=job_id).to_mongo().to_dict()

            expected_keys = [
                "_id",
                "user",
                "authstrat",
                "wsid",
                "status",
                "updated",
                "job_input",
                "scheduler_id",
                "child_jobs",
                "batch_job",
                "retry_ids",
                "retry_saved_toggle",
            ]
            self.assertCountEqual(job.keys(), expected_keys)

            # get job with exclude_fields
            job = (
                mongo_util.get_job(job_id=job_id, exclude_fields=["job_input"])
                .to_mongo()
                .to_dict()
            )

            expected_keys = [
                "_id",
                "user",
                "authstrat",
                "wsid",
                "status",
                "updated",
                "scheduler_id",
                "batch_job",
                "child_jobs",
                "retry_ids",
                "retry_saved_toggle",
            ]
            self.assertCountEqual(job.keys(), expected_keys)

            # get job with multiple exclude_fields
            job = (
                mongo_util.get_job(job_id=job_id, exclude_fields=["user", "wsid"])
                .to_mongo()
                .to_dict()
            )

            expected_keys = [
                "_id",
                "authstrat",
                "status",
                "updated",
                "job_input",
                "scheduler_id",
                "batch_job",
                "child_jobs",
                "retry_ids",
                "retry_saved_toggle",
            ]
            self.assertCountEqual(job.keys(), expected_keys)

            mongo_util.get_job(job_id=job_id).delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    def test_get_jobs_ok(self):
        mongo_util = self.getMongoUtil()

        with mongo_util.mongo_engine_connection():
            ori_job_count = Job.objects.count()
            job = get_example_job()
            job_id_1 = job.save().id
            job = get_example_job()
            job_id_2 = job.save().id
            self.assertEqual(ori_job_count, Job.objects.count() - 2)

            # get jobs with no exclude_fields
            jobs = mongo_util.get_jobs(job_ids=[job_id_1, job_id_2])

            expected_keys = [
                "_id",
                "user",
                "authstrat",
                "wsid",
                "status",
                "updated",
                "job_input",
                "scheduler_id",
                "batch_job",
                "child_jobs",
                "retry_ids",
                "retry_saved_toggle",
            ]

            for job in jobs:
                self.assertCountEqual(job.to_mongo().to_dict().keys(), expected_keys)

            # get jobs with multiple exclude_fields
            jobs = mongo_util.get_jobs(
                job_ids=[job_id_1, job_id_2], exclude_fields=["user", "wsid"]
            )

            expected_keys = [
                "_id",
                "authstrat",
                "status",
                "updated",
                "job_input",
                "scheduler_id",
                "batch_job",
                "child_jobs",
                "retry_ids",
                "retry_saved_toggle",
            ]
            for job in jobs:
                self.assertCountEqual(job.to_mongo().to_dict().keys(), expected_keys)

            mongo_util.get_job(job_id=job_id_1).delete()
            mongo_util.get_job(job_id=job_id_2).delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    def test_connection_ok(self):
        mongo_util = self.getMongoUtil()

        with mongo_util.mongo_engine_connection():
            ori_job_count = Job.objects.count()
            j = get_example_job()
            j.save()
            self.assertEqual(ori_job_count, Job.objects.count() - 1)

            job = mongo_util.get_job(job_id=j.id).to_mongo().to_dict()

            expected_keys = [
                "_id",
                "user",
                "authstrat",
                "wsid",
                "status",
                "updated",
                "job_input",
                "scheduler_id",
                "batch_job",
                "child_jobs",
                "retry_ids",
                "retry_saved_toggle",
            ]

            self.assertCountEqual(job.keys(), expected_keys)
            self.assertEqual(job["user"], j.user)
            self.assertEqual(job["authstrat"], "kbaseworkspace")
            self.assertEqual(job["wsid"], j.wsid)

            mongo_util.get_job(job_id=j.id).delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    def test_insert_one_ok(self):
        mongo_util = self.getMongoUtil()

        with mongo_util.pymongo_client(
            self.config["mongo-jobs-collection"]
        ) as pymongo_client:
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-jobs-collection"]
            ]

            ori_job_count = col.count_documents({})
            doc = {"test_key": "foo"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(ori_job_count, col.count_documents({}) - 1)

            result = list(col.find({"_id": ObjectId(job_id)}))[0]
            self.assertEqual(result["test_key"], "foo")

            col.delete_one({"_id": ObjectId(job_id)})
            self.assertEqual(col.count_documents({}), ori_job_count)

    def test_find_in_ok(self):
        mongo_util = self.getMongoUtil()

        with mongo_util.pymongo_client(
            self.config["mongo-jobs-collection"]
        ) as pymongo_client:
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-jobs-collection"]
            ]

            ori_job_count = col.count_documents({})
            doc = {"test_key_1": "foo", "test_key_2": "bar"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(ori_job_count, col.count_documents({}) - 1)

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
            self.assertEqual(col.count_documents({}), ori_job_count)

    def test_update_one_ok(self):
        mongo_util = self.getMongoUtil()

        with mongo_util.pymongo_client(
            self.config["mongo-jobs-collection"]
        ) as pymongo_client:
            col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-jobs-collection"]
            ]

            ori_job_count = col.count_documents({})
            doc = {"test_key_1": "foo"}
            job_id = mongo_util.insert_one(doc)
            self.assertEqual(ori_job_count, col.count_documents({}) - 1)

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
            self.assertEqual(col.count_documents({}), ori_job_count)

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

    def test_get_job_log_pymongo_ok(self):
        mongo_util = self.getMongoUtil()

        primary_key = ObjectId()

        jl = JobLog()
        jl.primary_key = primary_key
        jl.original_line_count = 0
        jl.stored_line_count = 0
        jl.lines = []

        with mongo_util.pymongo_client(
            self.config["mongo-jobs-collection"]
        ) as pymongo_client:
            jl_col = pymongo_client[self.config["mongo-database"]][
                self.config["mongo-logs-collection"]
            ]

            ori_jl_count = jl_col.count_documents({})

            jl.save()  # save job log

            self.assertEqual(JobLog.objects.count(), ori_jl_count + 1)
            job_log = mongo_util.get_job_log_pymongo(str(primary_key))

            self.assertEqual(job_log.get("original_line_count"), 0)
            self.assertEqual(job_log.get("stored_line_count"), 0)
            self.assertIsNone(job_log.get("lines"))
