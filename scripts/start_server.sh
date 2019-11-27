#!/bin/env bash
set -e
set -x
lib_dir=/kb/module/lib
wsgi_dir=$lib_dir/execution_engine2


# Set the number of gevent workers to number of cores * 2 + 1
# See: http://docs.gunicorn.org/en/stable/design.html#how-many-workers
calc_workers="$(($(nproc) * 2 + 1))"
# Use the WORKERS environment variable, if present
workers=${WORKERS:-$calc_workers}

export PYTHONPATH=$lib_dir:$wsgi_dir:$PYTHONPATH

gunicorn \
  --user kbase \
  --worker-class gevent \
  --timeout 1800 \
  --workers $workers  \
  --bind :5000 \
  ${DEVELOPMENT:+"--reload"} \
  execution_engine2Server:application