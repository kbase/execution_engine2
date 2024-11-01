#!/usr/bin/env bash
set -x

HOME=$(pwd)
export HOME

JOB_ID=$1
EE2_ENDPOINT=$2

# Remove 'services.' from EE2_ENDPOINT if it exists
EE2_ENDPOINT=${EE2_ENDPOINT/services./}

KBASE_ENDPOINT=$EE2_ENDPOINT
export KBASE_ENDPOINT

# Detect if we are running at NERSC and load some customization
if [ -e /global/homes/k/kbaserun/.local_settings ] ; then
   HOME=/global/homes/k/kbaserun
   . "$HOME/.local_settings" "$JOB_ID" "$EE2_ENDPOINT"
fi



debug_dir=$(mkdir -p "$(readlink -f debug)")


env >"${debug_dir}/envf"
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
} >>"${debug_dir}/env_file"

${PYTHON_EXECUTABLE} -V "${debug_dir}/pyversion"



tar -xf JobRunner.tgz && cd JobRunner && cp scripts/*.py . && chmod +x ./*.py


${PYTHON_EXECUTABLE} ./jobrunner.py "${JOB_ID}" "${EE2_ENDPOINT}" &
pid=$!

${PYTHON_EXECUTABLE} ./monitor_jobrunner_logs.py "${JOB_ID}" "${EE2_ENDPOINT}" "${pid}" &
pid2=$!

trap '{ kill $pid }' SIGTERM
wait ${pid}
EXIT_CODE=$?


kill -9 $pid2
exit ${EXIT_CODE}
