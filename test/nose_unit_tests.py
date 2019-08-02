import os
import sys
import execution_engine2
import nose
import logging

logging.basicConfig(level=logging.INFO)

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))


def begin():
    print("=" * 150)
    logging.info(f"About to run {test_name}")
    print("=" * 150)


if __name__ == "__main__":
    #   n = nose.run(argv='nose_tests_config.cfg')
    #    c = nose.config.Config

    nose.run(module="test")

    # file_path = os.path.abspath(__file__)
    # tests_path = os.path.join(os.path.abspath(os.path.dirname(file_path)), "")
    #
    # # result = nose.run(argv=[os.path.abspath(__file__),
    # #                         "--with-cov", "--verbosity=3", "--cover-package=execution_engine2",
    # #                         tests_path + "ee2_scheduler_test.py"])
    #
    # nose_params = [os.path.abspath(__file__), "--with-cov", "--stop", "--nocapture",
    #                "--nologcapture", "--with-coverage", "--cover-html",
    #
    #               ]
    # # "--cover-package=execution_engine2"
    # for test_name in [ "ee2_check_configure_mongo_docker.py"]:
    #     begin()
    #     params = nose_params
    #     params.append(tests_path + test_name)
    #     nose.run(argv=params)
