SERVICE = execution_engine2
SERVICE_CAPS = execution_engine2
SPEC_FILE = execution_engine2.spec
DIR = $(shell pwd)
LIB_DIR = lib
SCRIPTS_DIR = scripts
TEST_DIR = test

WORK_DIR = /kb/module/work/tmp

CONDOR_DOCKER_IMAGE_TAG_NAME = kbase/ee2:condor_test_instance


default: compile

all: compile build

compile:
	kb-sdk compile $(SPEC_FILE) \
		--out $(LIB_DIR) \
		--pysrvname $(SERVICE_CAPS).$(SERVICE_CAPS)Server \
		--pyimplname $(SERVICE_CAPS).$(SERVICE_CAPS)Impl;


	kb-sdk compile $(SPEC_FILE) \
		--out . \
		--html \

build:
	chmod +x $(SCRIPTS_DIR)/entrypoint.sh

TESTS := $(shell find . | grep test.py$ | grep tests_for | xargs)

setup-database:
	# Set up travis user in mongo
	PYTHONPATH=.:lib:test pytest --verbose /home/travis/virtualenv/ test/tests_for_db/ee2_check_configure_mongo_docker.py

test-coverage:
	# Set up travis user in mongo
	@echo "Run tests for $(TESTS)"
	PYTHONPATH=.:lib:test pytest --cov-report=xml --cov lib/execution_engine2/ --verbose $(TESTS)

build-condor-test-image:
	cd test/dockerfiles/condor && echo `pwd` && docker build -f Dockerfile . -t $(CONDOR_DOCKER_IMAGE_TAG_NAME)
	docker push $(CONDOR_DOCKER_IMAGE_TAG_NAME)
