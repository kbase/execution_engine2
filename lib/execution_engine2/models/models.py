import datetime
from enum import Enum

from mongoengine import (
    StringField,
    IntField,
    EmbeddedDocument,
    Document,
    DateTimeField,
    BooleanField,
    ListField,
    LongField,
    EmbeddedDocumentField,
    DynamicField,
    ValidationError,
    ObjectIdField,
    EmbeddedDocumentListField,
)


# TODO Make sure Datetime is correct format
# TODO Use ReferenceField to create a mapping between WSID and JOB IDS?
"""
"created": ISODate("2019-02-19T23:00:57.119Z"),
"updated": ISODate("2019-02-19T23:01:32.132Z"),
"""


class LogLines(EmbeddedDocument):
    line = StringField()
    linepos = IntField()
    error = BooleanField()


class JobLog(Document):
    primary_key = ObjectIdField(primary_key=True)
    updated = DateTimeField(default=datetime.datetime.utcnow)
    original_line_count = IntField()
    stored_line_count = IntField()
    lines = EmbeddedDocumentListField(LogLines)
    # meta = {"db_alias": "logs"}
    meta = {"collection": "ee2_logs"}


class Meta(EmbeddedDocument):
    run_id = StringField()
    token_id = StringField()
    tag = StringField()
    cell_id = StringField()
    status = StringField()


class JobInput(EmbeddedDocument):
    wsid = IntField(required=True)
    method = StringField(required=True)

    requested_release = StringField()
    params = DynamicField()
    service_ver = StringField(required=True)
    app_id = StringField(required=True)

    narrative_cell_info = EmbeddedDocumentField(Meta, required=True)


class JobOutput(EmbeddedDocument):
    version = StringField(required=True)
    id = LongField(required=True)
    result = DynamicField(required=True)


class Status(Enum):
    created = "created"
    estimating = "estimating"
    queued = "queued"
    running = "running"
    finished = "finished"
    error = "error"


class AuthStrat(Enum):
    kbaseworkspace = "kbaseworkspace"
    execution_engine = "execution_engine"


def valid_status(status):
    try:
        Status(status)
    except Exception as e:
        raise ValidationError(
            f"{status} is not a valid status {vars(Status)['_member_names_']}"
        )


def valid_authstrat(strat):
    if strat is None:
        pass
    try:
        AuthStrat(strat)
    except Exception as e:
        raise ValidationError(
            f"{strat} is not a valid Authentication strategy {vars(AuthStrat)['_member_names_']}"
        )


class Job(Document):
    user = StringField(required=True)
    authstrat = StringField(required=True, default="kbaseworkspace", validation=valid_authstrat)
    wsid = IntField(required=True)
    status = StringField(required=True, validation=valid_status)
    updated = DateTimeField(default=datetime.datetime.utcnow)
    started = DateTimeField(default=None)
    errormsg = StringField()
    scheduler_type = StringField()
    scheduler_id = StringField()
    job_input = EmbeddedDocumentField(JobInput, required=True)
    job_output = EmbeddedDocumentField(JobOutput)
    # meta = {"db_alias": "ee2"}
    meta = {"collection": "ee2_jobs"}


###
### Unused fields that we might want
###

result_example = {
    "shocknodes": [],
    "shockurl": "https://ci.kbase.us/services/shock-api/",
    "workspaceids": [],
    "workspaceurl": "https://ci.kbase.us/services/ws/",
    "results": [
        {
            "servtype": "Workspace",
            "url": "https://ci.kbase.us/services/ws/",
            "id": "psnovichkov:1450397093052/QQ",
            "desc": "description",
        }
    ],
    "prog": 0,
    "maxprog": None,
    "other": {
        "estcompl": None,
        "service": "bsadkhin",
        "desc": "Execution engine job for simpleapp.simple_add",
        "progtype": None,
    },
}


####
#### Unused Stuff to look at
####


class Results(EmbeddedDocument):
    run_id = StringField()
    shockurl = StringField()
    workspaceids = ListField()
    workspaceurl = StringField()
    shocknodes = ListField()
