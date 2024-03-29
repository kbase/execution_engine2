# -*- coding: utf-8 -*-
import logging
import os
import unittest

from bson.objectid import ObjectId
from pytest import raises

from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.db.models.models import Job, JobLog, Status
from test.utils_shared.test_utils import (
    bootstrap,
    get_example_job,
    read_config_into_dict,
    assert_exception_correct,
    assert_close_to_now,
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

    def test_update_job_to_queued_fail_with_bad_args(self):
        jid = "aaaaaaaaaaaaaaaaaaaaaaaa"
        err = ValueError("None of the 3 arguments can be falsy")
        self.update_job_to_queued_fail(None, "sid", "sch", err)
        self.update_job_to_queued_fail("", "sid", "sch", err)
        self.update_job_to_queued_fail(jid, None, "sch", err)
        self.update_job_to_queued_fail(jid, "", "sch", err)
        self.update_job_to_queued_fail(jid, "sid", None, err)
        self.update_job_to_queued_fail(jid, "sid", "", err)

    def update_job_to_queued_fail(self, job_id, schd_id, schd, expected):
        with raises(Exception) as got:
            self.getMongoUtil().update_job_to_queued(job_id, schd_id, schd)
        assert_exception_correct(got.value, expected)

    def test_update_job_to_queued(self):
        for state in Status:
            j = get_example_job(status=state.value)
            j.scheduler_id = None
            j.save()
            assert j.scheduler_id is None

            self.getMongoUtil().update_job_to_queued(j.id, "schdID", "condenast")
            j.reload()
            assert_close_to_now(j.updated)
            assert j.scheduler_id == "schdID"
            assert j.scheduler_type == "condenast"
            if state == Status.created:
                assert_close_to_now(j.queued)
                assert j.status == Status.queued.value
            else:
                assert j.queued is None
                assert j.status == state.value

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

    def test_get_job_log_pymongo_ok(self):
        mongo_util = self.getMongoUtil()

        primary_key = ObjectId()

        jl = JobLog()
        jl.primary_key = primary_key
        jl.original_line_count = 0
        jl.stored_line_count = 0
        jl.lines = []

        jl_col = mongo_util.pymongoc[self.config["mongo-database"]][
            self.config["mongo-logs-collection"]
        ]

        ori_jl_count = jl_col.count_documents({})

        jl.save()  # save job log

        self.assertEqual(JobLog.objects.count(), ori_jl_count + 1)
        job_log = mongo_util.get_job_log_pymongo(str(primary_key))

        self.assertEqual(job_log.get("original_line_count"), 0)
        self.assertEqual(job_log.get("stored_line_count"), 0)
        self.assertIsNone(job_log.get("lines"))
