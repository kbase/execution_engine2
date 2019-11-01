#!/usr/bin/env bash
set -x

#TODO Attempt to automatically find a compatible version of python with the correct libs
#OTHERWISE, PYTHON BIN IS $PYTHON_EXECUTABLE

export HOME=$(pwd)

env > envf
echo "export CLIENTGROUP=$CLIENTGROUP ">> env_file
echo "export PYTHON_EXECUTABLE=$PYTHON_EXECUTABLE ">> env_file
echo "export KB_ADMIN_AUTH_TOKEN=$KB_ADMIN_AUTH_TOKEN ">> env_file
echo "export KB_AUTH_TOKEN=$KB_AUTH_TOKEN ">> env_file
echo "export DOCKER_JOB_TIMEOUT=$DOCKER_JOB_TIMEOUT ">> env_file
echo "export CONDOR_ID=$CONDOR_ID ">> env_file
echo "export JOB_ID=$JOB_ID ">> env_file
echo "export DELETE_ABANDONED_CONTAINERS=$DELETE_ABANDONED_CONTAINERS ">> env_file


$PYTHON_EXECUTABLE -V > pyversion


JOB_ID=$1
EE2_ENDPOINT=$2
KBASE_ENDPOINT=$(EE2_ENDPOINT)
export KBASE_ENDPOINT

tar -xvf JobRunner.tgz && cd JobRunner && cp scripts/jobrunner.py . && chmod +x jobrunner.py

echo "$PYTHON_EXECUTABLE ./jobrunner.py ${JOB_ID} ${EE2_ENDPOINT}" > cmd

$PYTHON_EXECUTABLE ./jobrunner.py ${JOB_ID} ${EE2_ENDPOINT} > jobrunner.out 2> jobrunner.error &
pid=$!
#TODO TRAP IN PYTHON
#TODO MAKE SURE CONDOR_RM STILL WORKS IF PYTHON FAILS
#Trap condor_rm
# trap '{ kill $pid }' SIGTERM

wait ${pid}
EXIT_CODE=$?
exit ${EXIT_CODE}