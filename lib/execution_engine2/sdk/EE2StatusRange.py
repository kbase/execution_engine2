from collections import Counter
from collections import namedtuple
from datetime import datetime, timezone
from enum import Enum
from typing import Dict

from bson import ObjectId

from execution_engine2.utils.arg_processing import parse_bool
from execution_engine2.exceptions import AuthError


# TODO this class is duplicated all over the place, move to common file
class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


class JobStatusRange:
    def __init__(self, sdkmr):
        self.sdkmr = sdkmr

    @staticmethod
    def _create_stats(job_states: Dict) -> Dict:
        """
        Create a mapping of various stats to uniqify them and get counts of them
        :param job_states: Processed Job States
        :return:
        """
        stats = {}
        keys = ["clientgroup", "user", "app_id", "method", "wsid", "status"]
        for key in keys:
            stats[key] = Counter()

        for job in job_states:
            for key in keys:
                attribute = job.get(key)
                job_input = job.get("job_input", {})
                requirements = job_input.get("requirements", {})
                if attribute is None:
                    attribute = job_input.get(key)
                if attribute is None:
                    attribute = requirements.get(key)
                stats[key][attribute] += 1
        for key in keys:
            stats[key] = dict(stats[key])
        return stats

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

        if user is None:
            user = self.sdkmr.get_user_id()
        # Admins can view "ALL" or check_jobs for other users
        elif user != self.sdkmr.get_user_id():
            if not self.sdkmr.check_is_admin():
                raise AuthError(
                    "You are not authorized to view all records or records for others. "
                    + f"user={user} token={self.sdkmr.get_user_id()}"
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

        count = self.sdkmr.get_job_counts(job_filter_temp)
        jobs = self.sdkmr.get_jobs(
            job_filter_temp, job_projection, sort_order, offset, limit
        )

        self.sdkmr.get_logger().debug(
            f"Searching for jobs with id_gt {dummy_ids.start} id_lt {dummy_ids.stop}"
        )

        job_states = self._job_state_from_jobs(jobs)

        # Remove ObjectIds
        for item in job_filter_temp:
            job_filter_temp[item] = str(job_filter_temp[item])

        stats = self._create_stats(job_states)

        return {
            "jobs": job_states,
            "count": len(job_states),
            "query_count": count,
            "filter": job_filter_temp,
            "skip": offset,
            "projection": job_projection,
            "limit": limit,
            "sort_order": sort_order,
            "stats": stats,
        }

        # TODO Move to MongoUtils?
        # TODO Add support for projection (validate the allowed fields to project?) (Need better api design)
        # TODO Add support for filter (validate the allowed fields to project?) (Need better api design)
        # TODO USE AS_PYMONGO() FOR SPEED
        # TODO Better define default fields
        # TODO Instead of SKIP use ID GT LT https://www.codementor.io/arpitbhayani/fast-and-efficient-pagination-in-mongodb-9095flbqr
        # ^ this one is important - the workspace was DOSed by a single open narrative at one
        #   point due to skip abuse, which is why it was removed

    def _get_dummy_dates(self, creation_start_time, creation_end_time):

        if creation_start_time is None:
            raise Exception(
                "Please provide a valid start time for when job was created"
            )

        creation_start_time = self.sdkmr.check_and_convert_time(creation_start_time)
        creation_start_date = datetime.fromtimestamp(
            creation_start_time, tz=timezone.utc
        )
        dummy_start_id = ObjectId.from_datetime(creation_start_date)

        if creation_end_time is None:
            raise Exception("Please provide a valid end time for when job was created")

        creation_end_time = self.sdkmr.check_and_convert_time(creation_end_time)
        creation_end_date = datetime.fromtimestamp(creation_end_time, tz=timezone.utc)
        dummy_end_id = ObjectId.from_datetime(creation_end_date)

        if creation_start_time > creation_end_time:
            raise Exception("The start time cannot be greater than the end time.")

        dummy_ids = namedtuple("dummy_ids", "start stop")

        return dummy_ids(start=dummy_start_id, stop=dummy_end_id)

    def get_sort_order(self, ascending):
        if ascending is None:
            return "+"
        else:
            if parse_bool(ascending):
                return "+"
            else:
                return "-"

    @staticmethod
    def _job_state_from_jobs(jobs):
        """
        Returns as per the spec file

        :param jobs: MongoEngine Job Objects Query
        :return: list of job states of format
        Special Cases:
        str(_id)
        str(job_id)
        float(created/queued/estimating/running/finished/updated/) (Time in MS)
        """
        hidden_keys = ["retry_saved_toggle"]

        job_states = []
        for job in jobs:
            mongo_rec = job.to_mongo().to_dict()

            for key in hidden_keys:
                if key in mongo_rec:
                    del mongo_rec[key]

            mongo_rec["_id"] = str(job.id)
            mongo_rec["job_id"] = str(job.id)
            mongo_rec["created"] = int(job.id.generation_time.timestamp() * 1000)
            mongo_rec["updated"] = int(job.updated * 1000)
            if job.estimating:
                mongo_rec["estimating"] = int(job.estimating * 1000)
            if job.queued:
                mongo_rec["queued"] = int(job.queued * 1000)
            if job.running:
                mongo_rec["running"] = int(job.running * 1000)
            if job.finished:
                mongo_rec["finished"] = int(job.finished * 1000)
            job_states.append(mongo_rec)
        return job_states
