import time
from enum import Enum
from typing import Dict

from execution_engine2.db.models.models import (
    JobLog,
    LogLines,
)
from execution_engine2.exceptions import (
    RecordNotFoundException,
)


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"




def _create_new_log(pk):
    jl = JobLog()
    jl.primary_key = pk
    jl.original_line_count = 0
    jl.stored_line_count = 0
    jl.lines = []
    return jl


# @allow_job_write
def add_job_logs(sdkmr, job_id, log_lines, as_admin=False):
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

    sdkmr.logger.debug(f"About to add logs for {job_id}")
    mongo_util = sdkmr.get_mongo_util()

    try:
        log = mongo_util.get_job_log_pymongo(job_id)
    except RecordNotFoundException:
        log = _create_new_log(pk=job_id).to_mongo().to_dict()

    olc = log.get("original_line_count")

    for input_line in log_lines:
        olc += 1
        ll = LogLines()
        ll.error = input_line.get("error", False)
        ll.linepos = olc
        ts = input_line.get("ts")
        # TODO Maybe use strpos for efficiency?
        if ts is not None:
            ts = sdkmr._check_and_convert_time(ts, assign_default_time=True)

        ll.ts = ts

        ll.line = input_line.get("line")
        ll.validate()
        log["lines"].append(ll.to_mongo().to_dict())

    log["updated"] = time.time()
    log["original_line_count"] = olc
    log["stored_line_count"] = olc

    with mongo_util.pymongo_client(sdkmr.config["mongo-logs-collection"]):
        mongo_util.update_one(log, str(log.get("_id")))

    return log["stored_line_count"]

def _get_job_log(self, job_id, skip_lines) -> Dict:
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

    log = self.get_mongo_util().get_job_log_pymongo(job_id)

    lines = []
    for log_line in log.get("lines", []):  # type: LogLines
        if skip_lines and int(skip_lines) >= log_line.get("linepos", 0):
            continue
        lines.append(
            {
                "line": log_line.get("line"),
                "linepos": log_line.get("linepos"),
                "error": log_line.get("error"),
                "ts": int(log_line.get("ts", 0) * 1000),
            }
        )

    log_obj = {"lines": lines, "last_line_number": log["stored_line_count"]}
    return log_obj


# @allow_job_read
def view_job_logs(sdkmr, job_id, skip_lines):
    """
    Authorization Required: Ability to read from the workspace
    :param sdkmr: An instance of the SDK Method Runner, which contains the current request context
    :param job_id: The Job ID to view Jobs For
    :param skip_lines: An offset of the job logs
    :return:
    """


    return sdkmr._get_job_log(job_id, skip_lines)