# -*- coding: utf-8 -*-

import logging
import unittest

logging.basicConfig(level=logging.INFO)

from lib.installed_clients.execution_engine2Client import execution_engine2
from lib.installed_clients.WorkspaceClient import Workspace
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv("env/test.env", verbose=True)


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        This test is used for sending commands to a live environment, such as CI.
        TravisCI doesn't need to run this test.
        :return:
        """
        if "KB_AUTH_TOKEN" not in os.environ or "ENDPOINT" not in os.environ:
            logging.error(
                "Make sure you copy the env/test.env.example file to test/env/test.env and populate it"
            )
            sys.exit(1)

        cls.ee2 = execution_engine2(
            url=os.environ["EE2_ENDPOINT"], token=os.environ["KB_AUTH_TOKEN"]
        )
        cls.ws = Workspace(
            url=os.environ["WS_ENDPOINT"], token=os.environ["KB_AUTH_TOKEN"]
        )

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "wsName"):
            cls.wsClient.delete_workspace({"workspace": cls.wsName})
            print("Test workspace was deleted")

    def test_status(self):
        """
        Test to see if the ee2 service is up
        :return:
        """
        logging.info("Checking server time")
        self.assertIn("server_time", self.ee2.status())

    def test_config(self):
        conf = self.ee2.list_config({})
        for item in conf:
            print(item, conf[item])

    # def test_jobs_range(self):
    #     check_jobs_date_range_params = {}
    #     check_jobs_date_range_params = {""}
    #     self.ee2.check_jobs_date_range_for_user()
    #     self.ee2.check_jobs_date_range_for_all()

    def test_run_job(self):
        """
        Test a simple job based on runjob params from the spec file

        typedef structure {
            string method;
            list<UnspecifiedObject> params;
            string service_ver;
            RpcContext rpc_context;
            string remote_url;
            list<wsref> source_ws_objects;
            string app_id;
            mapping<string, string> meta;
            int wsid;
            string parent_job_id;
        } RunJobParams;

        :return:
        """
        params = {"base_number": "105"}
        runjob_params = {
            "method": "simpleapp.simple_add",
            "params": [params],
            "service_ver": "dev",
            "wsid": "42896",
            "app_id": "simpleapp",
        }

        job_id = self.ee2.run_job(runjob_params)
        print(f"Submitted job {job_id}")
        job_log_params = {"job_id": job_id}

        while True:
            time.sleep(5)
            try:
                print(self.ee2.get_job_status(job_id=job_id))
                print(self.ee2.get_job_logs(job_log_params))
                status = self.ee2.get_job_status(job_id=job_id)
                if status == {"status": "finished"}:
                    break
            except Exception as e:
                print("Not yet", e)


#         import datetime

#         now = datetime.datetime.utcnow()
#         line1 = {
#             "line": "Tell me the first line, the whole line, and nothing but the line",
#             "error": False,
#             "ts": now.isoformat(),
#         }
#         line2 = {"line": "This really crosses the line", "error": True}
#         lines = [line1, line2]

# self.ee2.add_job_logs(job_id, lines=lines)

# def test_add_job_log(self):
#     import datetime
#     job_id="5d51aa0517554f4cfd7b8a2b"
#     now = datetime.datetime.now()
#     line1 = {'line' : "1", 'error' : False, 'ts' : now}
#     line2 = {'line' : "1", 'error' : False}
#     lines = [line1,line2]
#
#     self.ee2.add_job_logs(job_id,lines=lines)

# def test_get_logs(self):
# job_id="5d51aa0517554f4cfd7b8a2b"
# job_log_params = {"job_id": job_id}
# print("About to get logs for", job_log_params)
# print(self.ee2.get_job_logs(job_log_params))

# def test_get_permissions(self):
#     username = 'bsadkhin'
#     perms = self.ws.get_permissions_mass({'workspaces': [{'id': '42896'}]})['perms']
#     permission = "n"
#     for p in perms:
#         if username in p:
#             permission = p[username]
#     print("Permission is")
#     print(permission)
#
# from enum import Enum
#
# class WorkspacePermissions(Enum):
#     ADMINISTRATOR = "a"
#     READ_WRITE = "w"
#     READ = "r"
#     NONE = "n"

# def test_get_workspace_permissions(self):
#     job_id='5d48821bfc8e83248c0d2cff'
#     wsid=42896
#     username="bsadkhin"
#     perms = self.ws.get_permissions_mass({'workspaces': [{'id': wsid}]})['perms']
#     permission = "n"
#     for p in perms:
#         if username in p:
#             permission = p[username]
#     print(self.WorkspacePermissions(permission))

# def get_workspace_permissions(self, wsid, ctx):
#         # Look up permissions for this workspace
#         logging.info(f"Checking for permissions for {wsid} of type {type(wsid)}")
#         gpmp = {'workspaces' : [int(wsid)]}
#         permission = self.get_workspace(ctx).get_permissions_mass(gpmp)[0]
#         return self.WorkspacePermissions(permission)
