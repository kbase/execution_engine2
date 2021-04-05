"""
Unit tests for the Condor wrapper.
"""

# TODO Add tests for get_job_resource_info and cancel_job

import htcondor
from unittest.mock import create_autospec, Mock

from execution_engine2.sdk.job_submission_parameters import (
    JobSubmissionParameters,
    JobRequirements,
)
from execution_engine2.utils.application_info import AppInfo
from execution_engine2.utils.user_info import UserCreds
from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.CondorTuples import SubmissionInfo

# TODO JRR fix this bug
# note the file existence code in the constructor appears to be buggy and will never throw an error.
# If it checks for existence of initial-dir/executable as well as just executable
# that makes testing a bit ungainly as executable will have to exist in the current # directory.


def _mock_htc():
    htc = create_autospec(htcondor, spec_set=True)
    sub = create_autospec(htcondor.Submit, spec_set=True, instance=True)
    htc.Submit.return_value = sub
    schedd = create_autospec(htcondor.Schedd, spec_set=True, instance=True)
    htc.Schedd.return_value = schedd
    txn = create_autospec(htcondor.Transaction, spec_set=True, instance=True)
    # mock context manager ops
    schedd.transaction.return_value = txn
    txn.__enter__.return_value = txn
    return htc, sub, schedd, txn


def _get_common_sub(job_id):
    return {
        "universe": "vanilla",
        "ShouldTransferFiles": "YES",
        "on_exit_hold": "ExitCode =!= 0",
        "JobLeaseDuration": "43200",
        "MaxJobRetirementTime": "43200",
        "Periodic_Hold": "( RemoteWallClockTime > 604800 )",
        "log": "runner_logs/$(Cluster).$(Process).log",
        "error": f"runner_logs/{job_id}.err",
        "output": f"runner_logs/{job_id}.out",
        "transfer_output_remaps": f'"runner_logs/{job_id}.err=cluster_logs/{job_id}.err;'
        + f'runner_logs/{job_id}.out=cluster_logs/{job_id}.out"',
        "When_To_Transfer_Output": "ON_EXIT_OR_EVICT",
        "getenv": "false",
    }


def _check_calls(htc, schedd, sub, txn, expected_sub):
    htc.Submit.assert_called_once_with(expected_sub)
    htc.Schedd.assert_called_once_with()
    schedd.transaction.assert_called_once_with()
    sub.queue.assert_called_once_with(txn, 1)


def test_run_job_minimal():
    htc, sub, schedd, txn = _mock_htc()
    c = Condor(
        {
            "external-url": "https://fake.com",
            "executable": "file.exe",
            "catalog-token": "cattoken",
        },
        htc=htc,
    )
    sub.queue.return_value = 123

    subinfo = c.run_job(
        JobSubmissionParameters(
            "jobbyjob",
            AppInfo("foo.bar", "foo/whoo"),
            JobRequirements(2, 3, 4, "cg"),
            UserCreds("user1", "token"),
        )
    )
    # presumably sub being part of the submission info is a bug. I assume that it's intended
    # to be the submission dictionary. However, that contains admin tokens and SubmissionInfo
    # gets logged so maybe it's better this way.
    assert subinfo == SubmissionInfo("123", sub, None)

    expected_sub = _get_common_sub("jobbyjob")
    expected_sub.update(
        {
            "JobBatchName": "jobbyjob",
            "arguments": "jobbyjob https://fake.com",
            "+KB_PARENT_JOB_ID": "",
            "+KB_MODULE_NAME": '"foo"',
            "+KB_FUNCTION_NAME": '"bar"',
            "+KB_APP_ID": '"foo/whoo"',
            "+KB_APP_MODULE_NAME": '"foo"',
            "+KB_WSID": "",
            "+KB_SOURCE_WS_OBJECTS": "",
            "request_cpus": "2",
            "request_memory": "3MB",
            "request_disk": "4GB",
            "requirements": 'regexp("cg",CLIENTGROUP)',
            "+KB_CLIENTGROUP": '"cg"',
            "Concurrency_Limits": "user1",
            "+AccountingGroup": '"user1"',
            "environment": (
                '"DOCKER_JOB_TIMEOUT=604801 KB_ADMIN_AUTH_TOKEN=cattoken KB_AUTH_TOKEN=token '
                + "CLIENTGROUP=cg JOB_ID=jobbyjob CONDOR_ID=$(Cluster).$(Process) "
                + 'PYTHON_EXECUTABLE=/miniconda/bin/python DEBUG_MODE=False PARENT_JOB_ID= "'
            ),
            "leavejobinqueue": "True",
            "initial_dir": "/condor_shared",
            "+Owner": '"condor_pool"',
            "executable": "/condor_shared/file.exe",
            "transfer_input_files": "/condor_shared/JobRunner.tgz",
        }
    )
    _check_calls(htc, schedd, sub, txn, expected_sub)


def test_run_job_maximal_with_concurrency_limits():
    """
    Tests with all constructor arguments and method arguments with concurrency limits.
    """
    _run_job_maximal(True, {})


def test_run_job_maximal_without_concurrency_limits():
    """
    Tests with all constructor arguments and method arguments without concurrency limits.
    """
    _run_job_maximal(False, {"Concurrency_Limits": "sucker"})


def _run_job_maximal(ignore_concurrency_limits, update):
    htc, sub, schedd, txn = _mock_htc()
    c = Condor(
        {
            "external-url": "https://fake2.com",
            "executable": "somefile.exe",
            "catalog-token": "catsupertoken",
            "PYTHON_EXECUTABLE": "python1.3",
            "initialdir": "/somedir",
            "docker_timeout": 42,
            "pool_user": "thosedamnkidsnextdoor",
            "leavejobinqueue": "False",
            "transfer_input_files": "alan_alda_nude.tiff",
        },
        htc=htc,
    )

    sub.queue.return_value = 789

    subinfo = c.run_job(
        JobSubmissionParameters(
            "a_job_id",
            AppInfo("kb_quast.run_quast_app", "kb_quast/run_QUAST_app"),
            JobRequirements(
                6,
                28,
                496,
                "clientclientclient",
                client_group_regex=False,
                bill_to_user="sucker",
                ignore_concurrency_limits=ignore_concurrency_limits,
                scheduler_requirements={"a": "b", "c": "d"},
                debug_mode=True,
            ),
            UserCreds("user2", "suparsekrit"),
            parent_job_id="old_n_gross",
            wsid=89,
            source_ws_objects=["1/2/3", "4/5/7"],
        )
    )
    # presumably sub being part of the submission info is a bug. I assume that it's intended
    # to be the submission dictionary. However, that contains admin tokens and SubmissionInfo
    # gets logged so maybe it's better this way.
    assert subinfo == SubmissionInfo("789", sub, None)

    expected_sub = _get_common_sub("a_job_id")
    expected_sub.update(update)
    expected_sub.update(
        {
            "JobBatchName": "a_job_id",
            "arguments": "a_job_id https://fake2.com",
            "+KB_PARENT_JOB_ID": '"old_n_gross"',
            "+KB_MODULE_NAME": '"kb_quast"',
            "+KB_FUNCTION_NAME": '"run_quast_app"',
            "+KB_APP_ID": '"kb_quast/run_QUAST_app"',
            "+KB_APP_MODULE_NAME": '"kb_quast"',
            "+KB_WSID": '"89"',
            "+KB_SOURCE_WS_OBJECTS": '"1/2/3,4/5/7"',
            "request_cpus": "6",
            "request_memory": "28MB",
            "request_disk": "496GB",
            "requirements": '(CLIENTGROUP == "clientclientclient") && (a == "b") && (c == "d")',
            "+KB_CLIENTGROUP": '"clientclientclient"',
            "+AccountingGroup": '"sucker"',
            "environment": (
                '"DOCKER_JOB_TIMEOUT=42 KB_ADMIN_AUTH_TOKEN=catsupertoken KB_AUTH_TOKEN=suparsekrit '
                + "CLIENTGROUP=clientclientclient JOB_ID=a_job_id CONDOR_ID=$(Cluster).$(Process) "
                + 'PYTHON_EXECUTABLE=python1.3 DEBUG_MODE=True PARENT_JOB_ID=old_n_gross "'
            ),
            "leavejobinqueue": "False",
            "initial_dir": "/somedir",
            "+Owner": '"thosedamnkidsnextdoor"',
            "executable": "/somedir/somefile.exe",
            "transfer_input_files": "alan_alda_nude.tiff",
        }
    )
    _check_calls(htc, schedd, sub, txn, expected_sub)
