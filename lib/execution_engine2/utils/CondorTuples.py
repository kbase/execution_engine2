from typing import NamedTuple, Optional, Dict


class JobInfo(NamedTuple):
    info: Optional[Dict]
    error: Optional[Exception]


class SubmissionInfo(NamedTuple):
    clusterid: Optional[str]
    submit: Dict
    error: Optional[Exception]


class CondorResources(NamedTuple):
    request_cpus: str
    request_disk: str
    request_memory: str
    client_group: str
