# -*- coding: utf-8 -*-
import copy
import logging
import os
import unittest
import threading
import queue
from configparser import ConfigParser
from unittest.mock import patch

from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.execution_engine2Impl import execution_engine2
from execution_engine2.SDKMethodRunner import SDKMethodRunner
from execution_engine2.utils.Condor import Condor, submission_info
from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.db.models.models import Job, Status
from test.mongo_test_helper import MongoTestHelper
from test.test_utils import bootstrap

logging.basicConfig(level=logging.INFO)
bootstrap()


class ee2_SDKMethodRunner_test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", "test/deploy.cfg")
        config_parser = ConfigParser()
        config_parser.read(config_file)

        cls.cfg = {}
        for nameval in config_parser.items("execution_engine2"):
            cls.cfg[nameval[0]] = nameval[1]

        mongo_in_docker = cls.cfg.get("mongo-in-docker-compose", None)
        if mongo_in_docker is not None:
            cls.cfg["mongo-host"] = cls.cfg["mongo-in-docker-compose"]

        cls.user_id = "wsadmin"
        cls.ws_id = 9999
        cls.token = "token"

        cls.ctx = {'token': cls.token,
                   'user_id': cls.user_id}
        cls.impl = execution_engine2(cls.cfg)
        cls.method_runner = SDKMethodRunner(
            cls.cfg, user_id=cls.user_id, token=cls.token
        )
        cls.mongo_util = MongoUtil(cls.cfg)
        cls.mongo_helper = MongoTestHelper(cls.cfg)

        cls.test_collection = cls.mongo_helper.create_test_db(
            db=cls.cfg["mongo-database"], col=cls.cfg["mongo-jobs-collection"]
        )

    def getRunner(self):
        return copy.deepcopy(self.__class__.method_runner)

    def get_sample_job_params(self, method=None):

        if not method:
            method = "default_method"

        job_params = {
            "wsid": self.ws_id,
            "method": method,
            "app_id": "MEGAHIT/run_megahit",
            "service_ver": "2.2.1",
            "params": [
                {
                    "workspace_name": "wjriehl:1475006266615",
                    "read_library_refs": ["18836/5/1"],
                    "output_contigset_name": "rhodo_contigs",
                    "recipe": "auto",
                    "assembler": None,
                    "pipeline": None,
                    "min_contig_len": None,
                }
            ],
            "parent_job_id": "9998",
            "meta": {"tag": "dev", "token_id": "12345"},
        }

        return job_params

    def test_init_job_stress(self):
        """
        testing initializing 3 different jobs in multiple theads.
        """

        thread_count = 3  # threads to test

        with self.mongo_util.mongo_engine_connection():
            ori_job_count = Job.objects.count()
            runner = self.getRunner()

            # set job method differently to distinguish
            method_1 = 'a_method'
            method_2 = 'b_method'
            job_params_1 = self.get_sample_job_params(method=method_1)
            job_params_2 = self.get_sample_job_params(method=method_2)

            threads = list()
            job_ids = list()
            que = queue.Queue()

            # execute _init_job_rec for 2 different jobs in threads
            for index in range(thread_count):
                x = threading.Thread(target=que.put(runner._init_job_rec(self.user_id, job_params_1)))
                threads.append(x)
                x.start()
                y = threading.Thread(target=que.put(runner._init_job_rec(self.user_id, job_params_2)))
                threads.append(y)
                y.start()

            for index, thread in enumerate(threads):
                thread.join()

            while not que.empty():
                job_ids.append(que.get())

            jobs = self.mongo_util.get_jobs(job_ids=job_ids)  # testing get jobs

            methods = [job.job_input.method for job in jobs]  # examing methods returned
            self.assertEqual(len(methods), thread_count * 2)
            self.assertEqual(methods.count(method_1), thread_count)
            self.assertEqual(methods.count(method_2), thread_count)

            self.assertEqual(len(set(job_ids)), thread_count * 2)  # testing identicalness of job_ids returned
            self.assertEqual(len(job_ids), len(set(job_ids)))

            self.assertEqual(ori_job_count, Job.objects.count() - thread_count * 2)   # testing job numbers created

            jobs.delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    def test_update_job_status_stress(self):
        """
        testing update jobs into different status in multiple threads
        """
        with self.mongo_util.mongo_engine_connection():
            ori_job_count = Job.objects.count()
            runner = self.getRunner()

            job_params = self.get_sample_job_params()

            thread_count = 3

            job_ids_queued = list()  # jobs to be set into 'queued' status
            job_ids_running = list()  # jobs to be set into 'running' status
            job_ids_completed = list()  # jobs to be set into 'completed' status

            # initializing jobs to be tested
            for index in range(thread_count):
                job_ids_queued.append(runner._init_job_rec(self.user_id, job_params))
                job_ids_running.append(runner._init_job_rec(self.user_id, job_params))
                job_ids_completed.append(runner._init_job_rec(self.user_id, job_params))

            # examing newly created job status
            queued_jobs = self.mongo_util.get_jobs(job_ids=job_ids_queued)
            for job in queued_jobs:
                job_rec = job.to_mongo().to_dict()
                self.assertIsNone(job_rec.get('queued'))
                self.assertEqual(job_rec.get('status'), 'created')

            running_jobs = self.mongo_util.get_jobs(job_ids=job_ids_running)
            for job in running_jobs:
                job_rec = job.to_mongo().to_dict()
                self.assertIsNone(job_rec.get('running'))
                self.assertEqual(job_rec.get('status'), 'created')

            finish_jobs = self.mongo_util.get_jobs(job_ids=job_ids_completed)
            for job in finish_jobs:
                job_rec = job.to_mongo().to_dict()
                self.assertIsNone(job_rec.get('finished'))
                self.assertEqual(job_rec.get('status'), 'created')

            threads = list()

            def update_states(index, job_ids_queued, job_ids_running, job_ids_finish):
                """
                update jobs status in one thread
                """
                runner.update_job_to_queued(job_ids_queued[index], 'scheduler_id')
                runner.start_job(job_ids_running[index])
                runner.start_job(job_ids_finish[index])
                job_output = {'version': '11', 'result': {'result': 1}, 'id': '5d54bdcb9b402d15271b3208'}
                runner.finish_job(job_ids_finish[index], job_output=job_output)

            for index in range(thread_count):
                x = threading.Thread(target=update_states(index, job_ids_queued, job_ids_running, job_ids_completed))
                threads.append(x)
                x.start()

            for index, thread in enumerate(threads):
                thread.join()

            # examing updateed job status
            queued_jobs = self.mongo_util.get_jobs(job_ids=job_ids_queued)
            for job in queued_jobs:
                job_rec = job.to_mongo().to_dict()
                self.assertIsNotNone(job_rec.get('queued'))
                self.assertEqual(job_rec.get('status'), 'queued')

            running_jobs = self.mongo_util.get_jobs(job_ids=job_ids_running)
            for job in running_jobs:
                job_rec = job.to_mongo().to_dict()
                self.assertIsNotNone(job_rec.get('running'))
                self.assertEqual(job_rec.get('status'), 'running')

            finish_jobs = self.mongo_util.get_jobs(job_ids=job_ids_completed)
            for job in finish_jobs:
                job_rec = job.to_mongo().to_dict()
                self.assertIsNotNone(job_rec.get('finished'))
                self.assertEqual(job_rec.get('status'), 'completed')

            jobs = self.mongo_util.get_jobs(job_ids=(job_ids_queued + job_ids_running + job_ids_completed))
            jobs.delete()
            self.assertEqual(ori_job_count, Job.objects.count())

    @patch.object(WorkspaceAuth, "can_write", return_value=True)
    @patch.object(SDKMethodRunner, "_get_client_groups", return_value="a group")
    @patch.object(SDKMethodRunner, "_get_module_git_commit", return_value="a version")
    @patch.object(Condor, "run_job", return_value=submission_info(clusterid="test", submit="job", error=None))
    def test_run_job(self, can_write, _get_client_groups, _get_module_git_commit, run_job):
        """
        testing running 3 different jobs in multiple theads.
        """
        thread_count = 3  # threads to test

        with self.mongo_util.mongo_engine_connection():
            ori_job_count = Job.objects.count()

            # set job method differently to distinguish
            method_1 = 'a_method'
            method_2 = 'b_method'
            method_3 = 'c_method'
            job_params_1 = self.get_sample_job_params(method=method_1)
            job_params_2 = self.get_sample_job_params(method=method_2)
            job_params_3 = self.get_sample_job_params(method=method_3)

            threads = list()
            job_ids = list()
            que = queue.Queue()

            # execute run_job for 3 different jobs in threads
            for index in range(thread_count):
                x = threading.Thread(target=que.put(self.impl.run_job(ctx=self.ctx, params=job_params_1)))
                threads.append(x)
                x.start()

                y = threading.Thread(target=que.put(self.impl.run_job(ctx=self.ctx, params=job_params_2)))
                threads.append(y)
                y.start()

                z = threading.Thread(target=que.put(self.impl.run_job(ctx=self.ctx, params=job_params_3)))
                threads.append(z)
                z.start()

            for index, thread in enumerate(threads):
                thread.join()

            while not que.empty():
                job_ids.append(que.get()[0])

            jobs = self.mongo_util.get_jobs(job_ids=job_ids)  # testing get jobs

            methods = [job.job_input.method for job in jobs]  # examing methods returned
            self.assertEqual(len(methods), thread_count * 3)
            self.assertEqual(methods.count(method_1), thread_count)
            self.assertEqual(methods.count(method_2), thread_count)
            self.assertEqual(methods.count(method_3), thread_count)

            status = [job.status for job in jobs]  # all jobs should eventually be put to 'queued' status
            self.assertCountEqual(status, [Status.queued.value] * thread_count * 3)

            self.assertEqual(len(set(job_ids)), thread_count * 3)  # testing identicalness of job_ids returned
            self.assertEqual(len(job_ids), len(set(job_ids)))

            self.assertEqual(ori_job_count, Job.objects.count() - thread_count * 3)  # testing job numbers created

            jobs.delete()
            self.assertEqual(ori_job_count, Job.objects.count())
