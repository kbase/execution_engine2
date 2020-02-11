import json
from typing import List, Dict

from lib.installed_clients.CatalogClient import Catalog


class CatalogUtils:
    def __init__(self, url):
        self.catalog = Catalog(url=url)

    def get_client_groups(self, method) -> Dict:
        """
        get client groups info from Catalog
        """
        if method is None:
            raise ValueError("Please input module_name.function_name")

        if method is not None and "." not in method:
            raise ValueError(
                "unrecognized method: {}. Please input module_name.function_name".format(
                    method
                )
            )

        module_name, function_name = method.split(".")

        group_config = self.catalog.list_client_group_configs(
            {"module_name": module_name, "function_name": function_name}
        )

        if len(group_config) == 0:
            group_config = {"client_groups": []}

        client_groups = group_config[0].get("client_groups", [])

        return self.normalize_catalog_cgroups(client_groups)

    @staticmethod
    def normalize_catalog_cgroups(resources_request: List):
        """
        Ensure that the client_groups are processed as a dictionary and has at least one value
        :param resources_request: either an empty string, a json object, or cg,key1=value,key2=value
        :return:
        """
        # No client group provided
        if len(resources_request) == 0:
            return {}
        # JSON
        if "{" in resources_request[0]:
            rr = ", ".join(resources_request)
            return json.loads(rr)

        rr = resources_request[0].split(",")
        rv = {"client_group": rr.pop(0)}
        for item in rr:
            if "=" not in item:
                raise Exception(
                    f"Malformed requirement. Format is <key>=<value> . Item is {item}"
                )
            (key, value) = item.split("=")
            rv[key] = value

        print("Going to return", rv)
        return rv
