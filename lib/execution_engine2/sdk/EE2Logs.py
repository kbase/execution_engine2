import time
from enum import Enum
from typing import Dict

from lib.execution_engine2.db.models.models import JobLog as JL, LogLines
from lib.execution_engine2.exceptions import RecordNotFoundException


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


class JobLog:
    def __init__(self, sdkmr):
        self.sdkmr = sdkmr

    @staticmethod
    def _create_new_log(pk):
        jl = JL()
        jl.primary_key = pk
        jl.original_line_count = 0
        jl.stored_line_count = 0
        jl.lines = []
        return jl

    # @allow_job_write
    def add_job_logs(self, job_id, log_lines, as_admin=False):
        """
        #Authorization Required : Ability to read and write to the workspace
        #Admin Authorization Required : Ability to Write to the workspace

        #TODO Prevent too many logs in memory
        #TODO Max size of log lines = 1000
        #TODO Error with out of space happened previously. So we just update line count.
        #TODO db.updateExecLogOriginalLineCount(ujsJobId, dbLog.getOriginalLineCount() + lines.size());


        # TODO Limit amount of lines per request?
        # TODO Maybe Prevent Some lines with TS and some without
        # TODO # Handle malformed requests?

        :param sdkmr:
        :param job_id:
        :param log_lines:
        :param as_admin:
        :return:
        """
        self.sdkmr.get_job_with_permission(
            job_id, JobPermissions.WRITE, as_admin=as_admin
        )

        self.sdkmr.logger.debug(f"About to add logs for {job_id}")
        mongo_util = self.sdkmr.get_mongo_util()

        try:
            log = mongo_util.get_job_log_pymongo(job_id)
        except RecordNotFoundException:
            log = self._create_new_log(pk=job_id).to_mongo().to_dict()

        olc = log.get("original_line_count")
        olc_temp = olc

        for input_line in log_lines:
            olc_temp += 1
            ll = LogLines()
            ll.error = int(input_line.get("is_error", 0)) == 1
            ll.linepos = olc_temp
            ts = input_line.get("ts")
            # TODO Maybe use strpos for efficiency?
            if ts is not None:
                ts = self.sdkmr.check_and_convert_time(ts, assign_default_time=True)

            ll.ts = ts

            ll.line = input_line.get("line")
            ll.validate()
            log["lines"].append(ll.to_mongo().to_dict())

        log["updated"] = time.time()
        log["original_line_count"] = olc_temp
        log["stored_line_count"] = olc_temp

        try:
            with mongo_util.pymongo_client(self.sdkmr.config["mongo-logs-collection"]):
                mongo_util.update_one(log, str(log.get("_id")))
        except Exception as e:
            self.sdkmr.logger.error(e)
            return olc

        return log["stored_line_count"]

    def _get_job_logs(self, job_id, skip_lines, limit=None) -> Dict:
        """
        # TODO Do I have to query this another way so I don't load all lines into memory?
        # Does mongoengine lazy-load it?

        # TODO IMPLEMENT SKIP LINES
        # TODO MAKE ONLY THE TIMESTAMP A STRING, so AS TO NOT HAVING TO LOOP OVER EACH ATTRIBUTE?
        # TODO Filter the lines in the mongo query?
        # TODO AVOID LOADING ENTIRE THING INTO MEMORY?
        # TODO Check if there is an off by one for line_count?


           :returns: instance of type "GetJobLogsResults" (last_line_number -
           common number of lines (including those in skip_lines parameter),
           this number can be used as next skip_lines value to skip already
           loaded lines next time.) -> structure: parameter "lines" of list
           of type "LogLine" -> structure: parameter "line" of String,
           parameter "is_error" of type "boolean" (@range [0,1]), parameter
           "last_line_number" of Long


        :param job_id:
        :param skip_lines:
        :return:
        """

        log = self.sdkmr.get_mongo_util().get_job_log_pymongo(job_id)

        lines = []
        last_line_number = 0
        count = len(log.get("lines", []))

        for log_line in log.get("lines", []):  # type: LogLines

            if skip_lines and int(skip_lines) >= log_line.get("linepos", 0):
                continue
            linepos = log_line.get("linepos")

            is_error = 0
            if log_line.get("error") is True:
                is_error = 1

            lines.append(
                {"line": log_line.get("line"), "linepos": linepos, "is_error": is_error}
            )
            ts = int(log_line.get("ts", 0) * 1000)
            jan_1_2010 = 1262307660
            if ts > jan_1_2010:
                lines[-1]["ts"] = ts

            last_line_number = max(int(linepos), last_line_number)
            if limit and limit <= len(lines):
                break

        if not lines:  # skipped all lines
            last_line_number = log["stored_line_count"]

        log_obj = {"lines": lines, "last_line_number": last_line_number, "count": count}
        return log_obj

    # @allow_job_read
    def view_job_logs(self, job_id, skip_lines, as_admin=False, limit=None):
        """
        Authorization Required: Ability to read from the workspace
        :param sdkmr: An instance of the SDK Method Runner, which contains the current request context
        :param job_id: The Job ID to view Jobs For
        :param skip_lines: An offset of the job logs
        :return:
        """
        # TODO Pass this into decorator?
        self.sdkmr.get_job_with_permission(
            job_id, JobPermissions.READ, as_admin=as_admin
        )

        return self._get_job_logs(job_id, skip_lines, limit)
