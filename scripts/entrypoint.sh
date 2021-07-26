#!/bin/bash

cp ./deploy.cfg ./work/config.properties

if [ $# -eq 0 ]; then
  useradd kbase
  if [ "${POOL_PASSWORD}" ]; then
    /usr/sbin/condor_store_cred -p "${POOL_PASSWORD}" -f /etc/condor/password
    chown kbase:kbase /etc/condor/password
  fi
  chown kbase /etc/condor/password

  # Copy downloaded JobRunner to a shared volume mount
  cp -rf /runner/JobRunner.tgz /condor_shared
  cp -rf ./scripts/execute_runner.sh /condor_shared

  # Give permissions to transfer logs into here
  mkdir /condor_shared/runner_logs && chown kbase /condor_shared/runner_logs
  mkdir /condor_shared/cluster_logs && chown kbase /condor_shared/cluster_logs

  # Save ENV Variables to file for cron and Remove _=/usr/bin/env
  env | sed 's/=\(.*\)/="\1"/' | head -n -1 | /etc/environment && chmod a+rw /etc/environment
  service cron start

  sh ./scripts/start_server.sh

elif [ "${1}" = "test" ]; then
  echo "Run Tests"
  make test

fi
