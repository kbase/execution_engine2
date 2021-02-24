"""
Unit tests for the EE2StatusRange class.
"""

from logging import Logger
from unittest.mock import create_autospec, call
from bson.objectid import ObjectId

from execution_engine2.sdk.SDKMethodRunner import SDKMethodRunner
from execution_engine2.sdk.EE2StatusRange import JobStatusRange
from execution_engine2.db.models.models import Job, JobInput, JobRequirements, Meta


# Incomplete by a long way. Will add more unit tests as they come up.

USER1 = "user1"


def test_run_minimal_no_user_in_input():
    """
    Tests a minimal run of the job lookup method as a standard user with no username passed into
    the method.
    The returned job has minimal fields.
    """
    _run_minimal(None)


def test_run_minimal_self_user_in_input():
    """
    Tests a minimal run of the job lookup method as a standard user with the user's own username
    passed into the method.
    The returned job has minimal fields.
    """
    _run_minimal(USER1)


def _run_minimal(user):
    # set up constants
    expected_user = USER1
    job_count = 26
    objectid = "603051cfaf2e3401b0500982"
    created_state = "created"

    # set up mock return values. Ordered as per the call order in the EE2SR code.
    sdkmr = create_autospec(SDKMethodRunner, spec_set=True, instance=True)
    logger = create_autospec(Logger, spec_set=True, instance=True)
    sdkmr.get_logger.return_value = logger
    sdkmr.get_user_id.return_value = expected_user
    sdkmr.check_and_convert_time.side_effect = [35.6, 92.4]
    sdkmr.get_job_counts.return_value = job_count

    j = Job()
    j.id = ObjectId(objectid)
    j.user = expected_user
    j.updated = 1000000.0
    j.status = created_state
    sdkmr.get_jobs.return_value = [j]

    # call the method
    ee2sr = JobStatusRange(sdkmr)
    ret = ee2sr.check_jobs_date_range_for_user("5/6/21", "7/6/21", user=user)

    assert ret == {
        'count': 1,
        'filter': {'id__gt': '000000230000000000000000',
                   'id__lt': '0000005c0000000000000000',
                   'user': expected_user},
        'jobs': [{'_id': objectid,
                  'authstrat': 'kbaseworkspace',
                  'batch_job': False,
                  'child_jobs': [],
                  'created': 1613779407000,
                  'job_id': objectid,
                  'status': created_state,
                  'updated': 1000000000,
                  'user': expected_user}],
        'limit': 2000,
        'projection': [],
        'query_count': job_count,
        'skip': 0,
        'sort_order': '+',
        'stats': {'app_id': {None: 1},
                  'clientgroup': {None: 1},
                  'method': {None: 1},
                  'status': {created_state: 1},
                  'user': {expected_user: 1},
                  'wsid': {None: 1}
                  }
    }

    # check mocks called as expected. Ordered as per the call order in the EE2SR code
    sdkmr.check_and_convert_time.assert_has_calls([call("5/6/21"), call("7/6/21")])
    expected_job_filter = {
        'id__gt': '000000230000000000000000',
        'id__lt': '0000005c0000000000000000',
        'user': expected_user
    }
    sdkmr.get_job_counts.assert_called_once_with(expected_job_filter)
    sdkmr.get_jobs.assert_called_once_with(expected_job_filter, [], '+', 0, 2000)
    logger.debug.assert_called_once_with(
        'Searching for jobs with id_gt 000000230000000000000000 id_lt 0000005c0000000000000000')
