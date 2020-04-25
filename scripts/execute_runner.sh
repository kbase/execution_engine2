#!/usr/bin/env bash
set -x

HOME=$(pwd)
export HOME

debug_dir="debug"
runner_logs_dir="runner_logs"
mkdir ${debug_dir}
mkdir ${runner_logs_dir}

env >${debug_dir}/envf
{
  echo "export CLIENTGROUP=$CLIENTGROUP "
  echo "export PYTHON_EXECUTABLE=$PYTHON_EXECUTABLE "
  echo "export KB_ADMIN_AUTH_TOKEN=$KB_ADMIN_AUTH_TOKEN "
  echo "export KB_AUTH_TOKEN=$KB_AUTH_TOKEN "
  echo "export DOCKER_JOB_TIMEOUT=$DOCKER_JOB_TIMEOUT "
  echo "export CONDOR_ID=$CONDOR_ID "
  echo "export JOB_ID=$JOB_ID "
  echo "export DELETE_ABANDONED_CONTAINERS=$DELETE_ABANDONED_CONTAINERS "
  echo "export DEBUG_MODE=$DEBUG_MODE "
} >>${debug_dir}/env_file

${PYTHON_EXECUTABLE} -V ${debug_dir}/pyversion

JOB_ID=$1
EE2_ENDPOINT=$2
KBASE_ENDPOINT=$(EE2_ENDPOINT)
export KBASE_ENDPOINT

tar -xvf JobRunner.tgz && cd JobRunner && cp scripts/jobrunner.py . && chmod +x jobrunner.py

cp scripts/monitor_jobrunner_logs.py . && chmod +x monitor_jobrunner_logs.py
echo "$PYTHON_EXECUTABLE ./jobrunner.py ${JOB_ID} ${EE2_ENDPOINT}" >${debug_dir}/cmd

${PYTHON_EXECUTABLE} ./jobrunner.py "${JOB_ID}" "${EE2_ENDPOINT}" >"${runner_logs_dir}/${JOB_ID}".out 2>"${runner_logs_dir}/${JOB_ID}".err &
pid=$!

echo "$PYTHON_EXECUTABLE ./monitor_jobrunner_logs.py ${JOB_ID} ${EE2_ENDPOINT} ${pid}" >${debug_dir}/cmd_log
#$PYTHON_EXECUTABLE ./monitor_jobrunner_logs.py ${JOB_ID} ${EE2_ENDPOINT} ${pid}

trap '{ kill $pid }' SIGTERM
wait ${pid}
EXIT_CODE=$?

LOG_DIR="../../../logs/${JOB_ID}"
mkdir -p "${LOG_DIR}"
mv "${runner_logs_dir}/*" "${LOG_DIR}/"

exit ${EXIT_CODE}
