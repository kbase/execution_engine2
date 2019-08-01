import logging
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import subprocess
import traceback
from bson.objectid import ObjectId


class MongoUtil:
    @classmethod
    def _start_local_service(self):
        logging.info("starting local mongod service")

        logging.info("running sudo service mongodb start")
        pipe = subprocess.Popen(
            "sudo service mongodb start",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout = pipe.communicate()
        logging.info(stdout)

        logging.info("running mongod --version")
        pipe = subprocess.Popen(
            "mongod --version",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout = pipe.communicate()
        logging.info(stdout)



    @classmethod
    def _get_collection(
        self,
        mongo_host,
        mongo_port,
        mongo_database,
        mongo_collection,
        mongo_user=None,
        mongo_password=None,
        mongo_authmechanism="DEFAULT",
    ):
        """
        connect Mongo server and return a collection
        """

        if mongo_user:
            logging.info(
                "mongo-user found in config file, configuring client for authentication using mech "
                + str(mongo_authmechanism)
            )
            my_client = MongoClient(
                mongo_host,
                mongo_port,
                username=mongo_user,
                password=mongo_password,
                authSource=mongo_database,
                authMechanism=mongo_authmechanism,
            )
        else:
            logging.info("no mongo-user found in config file, connecting without auth")
            my_client = MongoClient(mongo_host, mongo_port)

        try:
            my_client.server_info()  # force a call to server
        except ServerSelectionTimeoutError as e:
            error_msg = "Connot connect to Mongo server\n"
            error_msg += "ERROR -- {}:\n{}".format(
                e, "".join(traceback.format_exception(None, e, e.__traceback__))
            )
            raise ValueError(error_msg)

        # TODO: check potential problems. MongoDB will create the collection if it does not exist.
        my_database = my_client[mongo_database]

        print(my_database)
        print(mongo_collection)
        my_collection = my_database[mongo_collection]

        return my_collection

    def __init__(self, config):
        self.mongo_host = config["mongo-host"]
        self.mongo_port = int(config["mongo-port"])
        self.mongo_database = config["mongo-database"]
        self.mongo_user = config["mongo-user"]
        self.mongo_pass = config["mongo-password"]
        self.mongo_authmechanism = config["mongo-authmechanism"]

        self.mongo_collection = config["mongo-collection"]

        # self.jobs_collection = config["mongo-jobs-collection"]
        # self.logs_collection = config["mongo-logs-collection"]

        logging.info("Checking to see if we should start server")
        try:
            start_local = int(config.get("start-local-mongo", 1))
        except Exception:
            raise ValueError(
                "unexpected start-local-mongo: {}".format(
                    config.get("start-local-mongo")
                )
            )
        if start_local:
            logging.info("Starting Server")
            self._start_local_service()



        self.job_col = self._get_collection(
            self.mongo_host,
            self.mongo_port,
            self.mongo_database,
            self.mongo_collection,
            self.mongo_user,
            self.mongo_pass,
            self.mongo_authmechanism,
        )

        logging.basicConfig(
            format="%(created)s %(levelname)s: %(message)s", level=logging.INFO
        )

    def insert_one(self, doc):
        """
        insert a doc into collection
        """
        logging.info("start inserting document")

        try:
            rec = self.job_col.insert_one(doc)
        except Exception as e:
            error_msg = "Connot insert doc\n"
            error_msg += "ERROR -- {}:\n{}".format(
                e, "".join(traceback.format_exception(None, e, e.__traceback__))
            )
            raise ValueError(error_msg)

        return rec.inserted_id

    def update_one(self, doc, job_id):
        """
        update existing records
        https://docs.mongodb.com/manual/reference/operator/update/set/
        """
        logging.info("start updating document")

        try:
            update_filter = {"_id": ObjectId(job_id)}
            update = {"$set": doc}
            self.job_col.update_one(update_filter, update)
        except Exception as e:
            error_msg = "Connot update doc\n"
            error_msg += "ERROR -- {}:\n{}".format(
                e, "".join(traceback.format_exception(None, e, e.__traceback__))
            )
            raise ValueError(error_msg)

        return True

    def delete_one(self, doc, job_id):
        """
        delete a doc
        """
        logging.info("start deleting document")

        try:
            delete_filter = {"_id": ObjectId(job_id)}
            self.job_col.delete_one(delete_filter)
        except Exception as e:
            error_msg = "Connot delete doc\n"
            error_msg += "ERROR -- {}:\n{}".format(
                e, "".join(traceback.format_exception(None, e, e.__traceback__))
            )
            raise ValueError(error_msg)

        return True

    def find_in(self, elements, field_name, projection={"_id": False}, batch_size=1000):
        """
        return cursor that contains docs which field column is in elements
        """
        logging.info("start querying MongoDB")

        try:
            result = self.handle_collection.find(
                {field_name: {"$in": elements}},
                projection=projection,
                batch_size=batch_size,
            )
        except Exception as e:
            error_msg = "Connot query doc\n"
            error_msg += "ERROR -- {}:\n{}".format(
                e, "".join(traceback.format_exception(None, e, e.__traceback__))
            )
            raise ValueError(error_msg)

        logging.info("returned {} results".format(result.count()))

        return result
