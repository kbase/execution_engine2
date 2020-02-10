#!/usr/bin/env bash
set -x

export HOME=$(pwd)
$PYTHON_EXECUTABLE -V > pyversion


env > envf
echo "export CLIENTGROUP=$CLIENTGROUP ">> env_file
echo "export PYTHON_EXECUTABLE=$PYTHON_EXECUTABLE ">> env_file
echo "export KB_ADMIN_AUTH_TOKEN=$KB_ADMIN_AUTH_TOKEN ">> env_file
echo "export KB_AUTH_TOKEN=$KB_AUTH_TOKEN ">> env_file
echo "export DOCKER_JOB_TIMEOUT=$DOCKER_JOB_TIMEOUT ">> env_file
echo "export CONDOR_ID=$CONDOR_ID ">> env_file
echo "export JOB_ID=$JOB_ID ">> env_file
echo "export DELETE_ABANDONED_CONTAINERS=$DELETE_ABANDONED_CONTAINERS ">> env_file
echo "export DEBUG_MODE=$DEBUG_MODE ">> env_file


JOB_ID=$1
EE2_ENDPOINT=$2
KBASE_ENDPOINT=$(EE2_ENDPOINT)
export KBASE_ENDPOINT

tar -xvf JobRunner.tgz && cd JobRunner && cp scripts/jobrunner.py . && chmod +x jobrunner.py

cp scripts/monitor_jobrunner_logs.py . && chmod +x monitor_jobrunner_logs.py
echo "$PYTHON_EXECUTABLE ./jobrunner.py ${JOB_ID} ${EE2_ENDPOINT}" > cmd


$PYTHON_EXECUTABLE ./jobrunner.py ${JOB_ID} ${EE2_ENDPOINT} > jobrunner.out 2> jobrunner.err &
pid=$!

echo "$PYTHON_EXECUTABLE ./monitor_jobrunner_logs.py ${JOB_ID} ${EE2_ENDPOINT} ${pid}" > cmd_log
#$PYTHON_EXECUTABLE ./monitor_jobrunner_logs.py ${JOB_ID} ${EE2_ENDPOINT} ${pid}


trap '{ kill $pid }' SIGTERM
wait ${pid}
EXIT_CODE=$?

LOG_DIR=../../../logs/${JOB_ID}
mkdir -p $LOG_DIR
cp jobrunner.out $LOG_DIR/jobrunner.out
cp jobrunner.err $LOG_DIR/jobrunner.err


exit ${EXIT_CODE}