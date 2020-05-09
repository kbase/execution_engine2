#!/usr/bin/env bash
set -x

HOME=$(pwd)
export HOME

debug_dir="debug"
runner_logs="runner_logs"
mkdir ${debug_dir}
mkdir ${runner_logs}
runner_logs=$(readlink -f runner_logs)


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
KBASE_ENDPOINT=$EE2_ENDPOINT
export KBASE_ENDPOINT

tar -xf JobRunner.tgz && cd JobRunner && cp scripts/jobrunner.py . && chmod +x jobrunner.py

${PYTHON_EXECUTABLE} ./jobrunner.py "${JOB_ID}" "${EE2_ENDPOINT}" &
pid=$!


echo "$PYTHON_EXECUTABLE ./monitor_jobrunner_logs.py ${JOB_ID} ${EE2_ENDPOINT} ${pid}" >${debug_dir}/cmd_log
$PYTHON_EXECUTABLE ./monitor_jobrunner_logs.py ${JOB_ID} ${EE2_ENDPOINT} ${pid}

trap '{ kill $pid }' SIGTERM
wait ${pid}
EXIT_CODE=$?

# Deprecated these in favor of moving them back to ee2 container.
#LOG_DIR="../../../logs/${JOB_ID}"
#mkdir -p "${LOG_DIR}"
#cp "${runner_logs}/${JOB_ID}".out "${LOG_DIR}/".
#cp "${runner_logs}/${JOB_ID}".err "${LOG_DIR}/"
exit ${EXIT_CODE}
