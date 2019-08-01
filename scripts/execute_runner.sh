#!/usr/bin/env bash
set -x

#Trap condor_rm
trap "{ kill $pid }" SIGTERM

JOB_ID=$1
KBASE_ENDPOINT=$2
tar -xvf JobRunner.tgz && cd JobRunner && . ./venv/bin/activate && chmod +x jobrunner.py

./jobrunner.py ${JOB_ID} ${KBASE_ENDPOINT} > jobrunner.out 2> jobrunner.error &
pid=$!
wait ${pid}
EXIT_CODE=$?
exit ${EXIT_CODE}