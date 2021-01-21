import json
from collections import defaultdict
from typing import List, Dict, TYPE_CHECKING, NamedTuple

from lib.installed_clients.CatalogClient import Catalog

# if TYPE_CHECKING:
#     from lib.execution_engine2.utils.CondorTuples import CondorResources
#     from lib.execution_engine2.utils import Condor

class MethodVersion(NamedTuple):
    method: str
    version_request: str
    vcs: str

class CatalogUtils:
    def __init__(self, url, admin_token):
        self.catalog = Catalog(url=url, token=admin_token)
        self.method_version_cache = defaultdict(dict)


    

    def get_git_commit_version(self, job_params: Dict) -> str:
        """
        Convenience wrapper for verifying a git commit hash, or getting git commit hash from a tag
        :param params: Job Params (containing method and service_ver)
        :return: A git commit hash for the requested job
        """
        method = job_params["method"]
        service_ver = job_params.get("service_ver", "release")
        version = self.get_mass_git_commit_versions(job_param_set=[job_params])
        return version[method][service_ver]

    def get_mass_git_commit_versions(self, job_param_set: List[Dict]):
        """
        If "service_ver" is "release|beta|dev", get git commit version for that version
        if "service_ver" is a semantic version, get commit version for that semantic version
        If "service_ver" is a git commit hash, see if that get commit is valid

        :param job_param_set: List of batch job params (containing method and service_ver)
        :return: A cached mapping of method to version to git commit
        """
        # example
        # { 'run_megahit' :
        #   {
        #       'dev' : 'cc91ddfe376f907aa56cfb3dd1b1b21cae8885z6', #Tag
        #       '2.5.0' : 'cc91ddfe376f907aa56cfb3dd1b1b21cae8885z6', #Semantic
        #       'cc91ddfe376f907aa56cfb3dd1b1b21cae8885z6' : 'cc91ddfe376f907aa56cfb3dd1b1b21cae8885z6' #vcs
        #    }
        # }

        for param in job_param_set:
            method = param["method"]
            module_name = method.split(".")[0]
            service_ver = param.get("service_ver", "release")
            if module_name not in self.method_version_cache or service_ver not in self.method_version_cache[method]:
                module_version = self.catalog.get_module_version(
                    {"module_name": module_name, "version": service_ver}
                )
                self.method_version_cache[method][service_ver] = module_version.get(
                    "git_commit_hash"
                )
        return self.method_version_cache

    def get_mass_resources(
        self, job_param_set: List[Dict], condor
    ) -> Dict:
        """
        Gets a list of required condor resources and clientgroups for a set of jobs

        :param job_param_set: List of batch job params
        :param condor: Instance of condor utils # type: Condor
        :return: A cached mapping of method to extracted resources # type: Dict[str:CondorResources]
        """
        condor_resources = dict()
        for param in job_param_set:
            method = param["method"]
            if method not in condor_resources:
                normalized_resources = self.get_normalized_resources(method=method)
                extracted_resources = condor.extract_resources(
                    cgrr=normalized_resources
                )  # type: CondorResources
                condor_resources[method] = extracted_resources
        return condor_resources

    def get_normalized_resources(self, method: str) -> Dict:
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

        job_settings = []
        if group_config and len(group_config) > 0:
            job_settings = group_config[0].get("client_groups")

        normalize = self.normalize_job_settings(job_settings)

        return normalize

    @staticmethod
    def normalize_job_settings(resources_request: List):
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
            json_resources_request = ", ".join(resources_request)
            return json.loads(json_resources_request)
        # CSV Format
        rr = resources_request[0].split(",")  # type: list
        rv = {"client_group": rr.pop(0)}
        for item in rr:
            if "=" not in item:
                raise Exception(
                    f"Malformed requirement. Format is <key>=<value> . Item is {item}"
                )
            (key, value) = item.split("=")
            rv[key] = value
        #
        # print("Going to return", rv)
        return rv
