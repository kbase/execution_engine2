#!/bin/bash

if [ $# -eq 0 ] ; then
  useradd kbase
  if [ "${POOL_PASSWORD}" ] ; then
        /usr/sbin/condor_store_cred -p "${POOL_PASSWORD}" -f /etc/condor/password
  fi
  chown kbase /etc/condor/password
  cp -rf /runner/JobRunner.tgz /condor_shared
  cp -rf ./scripts/execute_runner.sh /condor_shared
  sh ./scripts/start_server.sh

elif [ "${1}" = "test" ] ; then
  echo "Run Tests"
  make test

fi
