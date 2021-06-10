from collections import defaultdict
from typing import Dict

from lib.installed_clients.CatalogClient import Catalog


class CatalogCache:
    """
    Per call catalog cache used to speed up catalog lookups
    Caches the "Method Version" and the "Job Resource Requirements"
    There's no cache invalidation, and to refresh a cache entry you have to make a new cache
    Cache is not thread safe
    """

    def __init__(self, catalog: Catalog):
        """
        :param catalog: Instance of catalog client. Does not require authentication
        """
        if not catalog:
            raise ValueError("Please provide instance of catalog client")

        self._catalog = catalog
        self._method_version_cache = defaultdict(dict)
        self._job_requirements_cache = defaultdict(dict)

    def get_catalog(self) -> Catalog:
        """Get the catalog client for this instance."""
        return self._catalog

    def get_method_version_cache(self) -> Dict:
        """Get the _method_version_cache for this instance."""
        return self._method_version_cache

    def get_job_resources_cache(self) -> Dict:
        """Get the _condor_resources_cache for this instance."""
        return self._job_requirements_cache

    def get_git_commit_version(self, method, service_ver=None) -> str:
        """
        If "service_ver" is "release|beta|dev", get git commit version for that version
        if "service_ver" is a semantic version, get commit version for that semantic version
        If "service_ver" is a git commit hash, see if that get commit is valid
        Convenience wrapper for verifying a git commit hash, or getting git commit hash from a tag
        :param method: Method to look up
        :param service_ver: Version to look up
        :return: A git commit hash for the requested job
        """

        # Structure of cache
        # { 'run_megahit' :
        #   {
        #       'dev' : 'cc91ddfe376f907aa56cfb3dd1b1b21cae8885z6', #Tag
        #       '2.5.0' : 'cc91ddfe376f907aa56cfb3dd1b1b21cae8885z6', #Semantic
        #       'cc91ddfe376f907aa56cfb3dd1b1b21cae8885z6' : 'cc91ddfe376f907aa56cfb3dd1b1b21cae8885z6' #vcs
        #    }
        # }
        mv_cache = self.get_method_version_cache()
        if not method:
            raise ValueError("Must provide a method to lookup")

        if not service_ver:
            service_ver = "release"

        # If not in the cache add it
        if method not in mv_cache or service_ver not in mv_cache[method]:
            module_name = method.split(".")[0]
            module_version = self.get_catalog().get_module_version(
                {"module_name": module_name, "version": service_ver}
            )
            mv_cache[method][service_ver] = module_version.get("git_commit_hash")
        # Retrieve from cache
        return mv_cache[method][service_ver]

    def lookup_job_resource_requirements(self, module_name, function_name) -> dict:
        """
        Gets required job resources and clientgroups for a job submission
        :param module_name: Module name to lookup
        :param function_name: Function name to lookup
        :return: A cached lookup of unformatted resource requests from the catalog
        """
        # Structure of cache
        # { 'module_name' : {'function_name' : [group_config] }
        # }
        cr_cache = self.get_job_resources_cache()
        # If not in the cache add it
        if module_name not in cr_cache or function_name not in cr_cache[module_name]:
            cr_cache[module_name][
                function_name
            ] = self.get_catalog().list_client_group_configs(
                {"module_name": module_name, "function_name": function_name}
            )
        # Retrieve from cache
        return cr_cache[module_name][function_name]
