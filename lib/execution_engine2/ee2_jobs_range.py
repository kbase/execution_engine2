import logging
import logging
import time
from collections import namedtuple
from datetime import datetime
from enum import Enum

import dateutil
from bson import ObjectId

from execution_engine2.db.models.models import (
    Job,
)
from execution_engine2.exceptions import AuthError


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"





  def check_jobs_date_range_for_user(
        self,
        creation_start_time,
        creation_end_time,
        job_projection=None,
        job_filter=None,
        limit=None,
        user=None,
        offset=None,
        ascending=None,
    ):

        """
        :param creation_start_time: Start timestamp since epoch for Creation
        :param creation_end_time: Stop timestamp since epoch for Creation
        :param job_projection:  List of fields to project alongside [_id, authstrat, updated, created, job_id]
        :param job_filter:  List of simple job fields of format key=value
        :param limit: Limit of records to return, default 2000
        :param user: Optional Username or "ALL" for all users
        :param offset: Optional offset for skipping records
        :param ascending: Sort by id ascending or descending
        :return:
        """
        sort_order = self.get_sort_order(ascending)

        if offset is None:
            offset = 0

        if self.token is None:
            raise AuthError("Please provide a token to check jobs date range")

        token_user = self.auth.get_user(self.token)
        if user is None:
            user = token_user

        # Admins can view "ALL" or check_jobs for other users
        if user != token_user:
            if not self._is_admin(self.token):
                raise AuthError(
                    f"You are not authorized to view all records or records for others. user={user} token={token_user}"
                )

        dummy_ids = self._get_dummy_dates(creation_start_time, creation_end_time)

        if job_projection is None:
            # Maybe set a default here?
            job_projection = []

        if not isinstance(job_projection, list):
            raise Exception("Invalid job projection type. Must be list")

        if limit is None:
            # Maybe put this in config
            limit = 2000

        job_filter_temp = {}
        if isinstance(job_filter, list):
            for item in job_filter:
                (k, v) = item.split("=")
                job_filter_temp[k] = v
        elif isinstance(job_filter, dict):
            job_filter_temp = job_filter
        elif job_filter is None:
            pass
        else:
            raise Exception(
                "Job filter must be a dictionary or a list of key=value pairs"
            )

        job_filter_temp["id__gt"] = dummy_ids.start
        job_filter_temp["id__lt"] = dummy_ids.stop

        if user != "ALL":
            job_filter_temp["user"] = user

        with self.get_mongo_util().mongo_engine_connection():
            count = Job.objects.filter(**job_filter_temp).count()
            jobs = (
                Job.objects[:limit]
                .filter(**job_filter_temp)
                .order_by(f"{sort_order}_id")
                .skip(offset)
                .only(*job_projection)
            )

        logging.info(
            f"Searching for jobs with id_gt {dummy_ids.start} id_lt {dummy_ids.stop}"
        )

        job_states = self._job_state_from_jobs(jobs)

        # Remove ObjectIds
        for item in job_filter_temp:
            job_filter_temp[item] = str(job_filter_temp[item])

        return {
            "jobs": job_states,
            "count": len(job_states),
            "query_count": count,
            "filter": job_filter_temp,
            "skip": offset,
            "projection": job_projection,
            "limit": limit,
            "sort_order": sort_order,
        }

        # TODO Move to MongoUtils?
        # TODO Add support for projection (validate the allowed fields to project?) (Need better api design)
        # TODO Add support for filter (validate the allowed fields to project?) (Need better api design)
        # TODO USE AS_PYMONGO() FOR SPEED
        # TODO Better define default fields
        # TODO Instead of SKIP use ID GT LT https://www.codementor.io/arpitbhayani/fast-and-efficient-pagination-in-mongodb-9095flbqr

    def _get_dummy_dates(self, creation_start_time, creation_end_time):

        if creation_start_time is None:
            raise Exception(
                "Please provide a valid start time for when job was created"
            )

        creation_start_time = self._check_and_convert_time(creation_start_time)
        creation_start_date = datetime.fromtimestamp(creation_start_time)
        dummy_start_id = ObjectId.from_datetime(creation_start_date)

        if creation_end_time is None:
            raise Exception("Please provide a valid end time for when job was created")

        creation_end_time = self._check_and_convert_time(creation_end_time)
        creation_end_date = datetime.fromtimestamp(creation_end_time)
        dummy_end_id = ObjectId.from_datetime(creation_end_date)

        if creation_start_time > creation_end_time:
            raise Exception("The start time cannot be greater than the end time.")

        dummy_ids = namedtuple("dummy_ids", "start stop")

        return dummy_ids(start=dummy_start_id, stop=dummy_end_id)

    @staticmethod
    def get_sort_order(ascending):
        if ascending is None:
            return "+"
        else:
            if SDKMethodRunner.parse_bool_from_string(ascending):
                return "+"
            else:
                return "-"

