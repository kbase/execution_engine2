from enum import Enum
from typing import NamedTuple

_JOB_REQUIREMENTS = "job_reqs"
_JOB_REQUIREMENTS_INCOMING = "job_requirements"
_SCHEDULER_REQUIREMENTS = "scheduler_requirements"
_META = "meta"  # narrative_cell_info
_APP_PARAMS = "params"  # application parameters
_REQUIREMENTS_LIST = "requirements_list"
_METHOD = "method"
_APP_ID = "app_id"
_BATCH_ID = "batch_id"
_PARENT_JOB_ID = "parent_job_id"
_PARENT_RETRY_JOB_ID = "retry_parent"
_RETRY_IDS = "retry_ids"
_WORKSPACE_ID = "wsid"
_SOURCE_WS_OBJECTS = "source_ws_objects"
_SERVICE_VER = "service_ver"


class JobPermissions(Enum):
    READ = "r"
    WRITE = "w"
    NONE = "n"


class PreparedJobParams(NamedTuple):
    params: dict
    job_id: str


class JobIdPair(NamedTuple):
    job_id: str
    scheduler_id: str