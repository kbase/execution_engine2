import json
import logging
import os
import time
from collections import namedtuple, OrderedDict
from datetime import datetime
from enum import Enum
from logging import Logger
from typing import Dict, AnyStr

import dateutil
from bson import ObjectId
from cachetools import TTLCache

from execution_engine2.authorization.authstrategy import (
    can_read_job,
    can_read_jobs,
    can_write_job,
)
from execution_engine2.authorization.roles import AdminAuthUtil
from execution_engine2.authorization.workspaceauth import WorkspaceAuth
from execution_engine2.db.MongoUtil import MongoUtil
from execution_engine2.db.models.models import (
    Job,
    JobInput,
    JobOutput,
    Meta,
    Status,
    JobLog,
    LogLines,
    ErrorCode,
    TerminatedCode,
    JobRequirements,
)
from execution_engine2.exceptions import AuthError
from execution_engine2.exceptions import (
    RecordNotFoundException,
    InvalidStatusTransitionException,
)
from execution_engine2.utils.CatalogUtils import CatalogUtils
from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.Condor import condor_resources
from execution_engine2.utils.KafkaUtils import (
    KafkaClient,
    KafkaCancelJob,
    KafkaCondorCommand,
    KafkaFinishJob,
    KafkaStartJob,
    KafkaStatusChange,
    KafkaCreateJob,
    KafkaQueueChange,
)
from execution_engine2.utils.SlackUtils import SlackClient
from installed_clients.WorkspaceClient import Workspace
from installed_clients.authclient import KBaseAuth


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


import ee2_runjob
import ee2_status
import ee2_cache
import ee2_logs

class SDKMethodRunner:
    JOB_PERMISSION_CACHE_SIZE = 500
    JOB_PERMISSION_CACHE_EXPIRE_TIME = 300  # seconds
    ADMIN_READ_ROLE = 'EE2_ADMIN_RO'
    ADMIN_WRITE_ROLE = 'EE2_ADMIN'

    def __init__(self, config, user_id=None, token=None, job_permission_cache=None, roles_cache=None):
        self.deployment_config_fp = os.environ["KB_DEPLOYMENT_CONFIG"]
        self.config = config
        self.mongo_util = None
        self.condor = None
        self.workspace = None
        self.workspace_auth = None
        self.admin_roles = config.get("admin_roles", ["EE2_ADMIN", "EE2_ADMIN_RO"])
        self.catalog_utils = CatalogUtils(config.get("catalog-url"))
        self.workspace_url = config.get("workspace-url")
        self.auth_url = config.get("auth-url")
        self.auth = KBaseAuth(auth_url=config.get("auth-service-url"))
        self.user_id = user_id
        self.token = token
        self.debug = SDKMethodRunner.parse_bool_from_string(config.get("debug"))
        self.logger = self._set_log_level()

        self.job_permission_cache = ee2_cache.get_cache(cache=job_permission_cache, size=self.JOB_PERMISSION_CACHE_SIZE, expire=self.JOB_PERMISSION_CACHE_EXPIRE_TIME)
        self.roles_cache = ee2_cache.get_cache(cache=roles_cache, size=self.JOB_PERMISSION_CACHE_SIZE, expire=self.JOB_PERMISSION_CACHE_EXPIRE_TIME)
        self.is_admin = False
        # self.roles = self.roles_cache.get_roles(user_id,token) or list()
        self.kafka_client = KafkaClient(config.get("kafka-host"))
        self.slack_client = SlackClient(config.get("slack-token"), debug=self.debug)


    def allow_job_read(func):
        def inner(self, *args, **kwargs):
            job_id = kwargs.get("job_id")
            if job_id is None:
                raise ValueError("Please provide valid job_id")
            self._test_job_permission_with_cache(job_id, JobPermissions.READ)

            return func(self, *args, **kwargs)

        return inner

    def allow_job_write(func):
        def inner(self, *args, **kwargs):
            job_id = kwargs.get("job_id")
            if job_id is None:
                raise ValueError("Please provide valid job_id")
            self._test_job_permission_with_cache(job_id, JobPermissions.WRITE)

            return func(self, *args, **kwargs)

        return inner

    def get_workspace_auth(self) -> WorkspaceAuth:
        if self.workspace_auth is None:
            self.workspace_auth = WorkspaceAuth(
                self.token, self.user_id, self.workspace_url
            )
        return self.workspace_auth

    def get_mongo_util(self) -> MongoUtil:
        if self.mongo_util is None:
            self.mongo_util = MongoUtil(self.config)
        return self.mongo_util

    def get_condor(self) -> Condor:
        if self.condor is None:
            self.condor = Condor(self.deployment_config_fp)
        return self.condor

    def get_workspace(self) -> Workspace:
        if self.workspace is None:
            self.workspace = Workspace(token=self.token, url=self.workspace_url)
        return self.workspace


    def _set_log_level(self) -> Logger:
        """
        Enable this setting to get output for development purposes
        Otherwise, only emit warnings or errors for production
        """
        log_format = "%(created)s %(levelname)s: %(message)s"
        logger = logging.getLogger("ee2")
        fh = logging.StreamHandler()
        fh.setFormatter(logging.Formatter(log_format))
        fh.setLevel(logging.WARN)

        if self.debug:
            fh.setLevel(logging.DEBUG)

        logger.addHandler(fh)
        # logging.warning(f"DEBUG is {self.debug}. T=(debug/info/w/e) F=(warning/error)")

        return logger

    def run_job(self, params, as_admin=False):
        return ee2_runjob.run(sdkmr=self, params=params, as_admin=as_admin)

    def get_job_params(self, job_id, as_admin=False):
        return ee2_runjob.get_job_params(sdkmr=self, job_id=job_id, as_admin=as_admin)

    def add_job_logs(self, job_id, log_lines, as_admin=False):
        return ee2_logs.add_job_logs(sdkmr=self, job_id=job_id, log_lines=log_lines, as_admin=as_admin)


    @staticmethod
    def parse_bool_from_string(str_or_bool):
        if isinstance(str_or_bool, bool):
            return str_or_bool

        if isinstance(str_or_bool, int):
            return str_or_bool

        if isinstance(json.loads(str_or_bool.lower()), bool):
            return json.loads(str_or_bool.lower())

        raise Exception("Not a boolean value")

    @staticmethod
    def _check_and_convert_time(time_input, assign_default_time=False):
        """
        convert input time into timestamp in epoch format
        """

        try:
            if isinstance(time_input, str):  # input time_input as string
                if time_input.replace(
                        ".", "", 1
                ).isdigit():  # input time_input as numeric string
                    time_input = (
                        float(time_input)
                        if "." in time_input
                        else int(time_input) / 1000.0
                    )
                else:  # input time_input as datetime string
                    time_input = dateutil.parser.parse(time_input).timestamp()
            elif isinstance(
                    time_input, int
            ):  # input time_input as epoch timestamps in milliseconds
                time_input = time_input / 1000.0
            elif isinstance(time_input, datetime):
                time_input = time_input.timestamp()

            datetime.fromtimestamp(time_input)  # check current time_input is valid
        except Exception:
            if assign_default_time:
                logging.info(
                    "Cannot convert time_input into timestamps: {}".format(time_input)
                )
                time_input = time.time()
            else:
                raise ValueError(
                    "Cannot convert time_input into timestamps: {}".format(time_input)
                )

        return time_input
