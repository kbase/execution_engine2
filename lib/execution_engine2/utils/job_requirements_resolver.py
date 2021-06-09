"""
Contains resolvers for job requirements.
"""

import json
from configparser import ConfigParser
from enum import Enum
from typing import Iterable, Dict, Union, Set

from execution_engine2.exceptions import IncorrectParamsException
from execution_engine2.sdk.EE2Constants import (
    EE2_CONFIG_SECTION,
    EE2_DEFAULT_SECTION,
    EE2_DEFAULT_CLIENT_GROUP,
)
from execution_engine2.sdk.job_submission_parameters import JobRequirements
from execution_engine2.utils.arg_processing import (
    check_string as _check_string,
    not_falsy as _not_falsy,
)
from execution_engine2.utils.catalog_cache import CatalogCache
from lib.installed_clients.CatalogClient import Catalog

CLIENT_GROUP = "client_group"
REQUEST_CPUS = "request_cpus"
REQUEST_MEMORY = "request_memory"
REQUEST_DISK = "request_disk"
CLIENT_GROUP_REGEX = "client_group_regex"
BILL_TO_USER = "bill_to_user"
IGNORE_CONCURRENCY_LIMITS = "ignore_concurrency_limits"
DEBUG_MODE = "debug_mode"
_RESOURCES = set([CLIENT_GROUP, REQUEST_CPUS, REQUEST_MEMORY, REQUEST_DISK])
_ALL_SPECIAL_KEYS = _RESOURCES | set(
    [CLIENT_GROUP_REGEX, DEBUG_MODE, BILL_TO_USER, IGNORE_CONCURRENCY_LIMITS]
)

_CLIENT_GROUPS = "client_groups"


def _remove_special_keys(inc_dict):
    return {k: inc_dict[k] for k in set(inc_dict) - _ALL_SPECIAL_KEYS}


class RequirementsType(Enum):
    """
    A classification of the type of requirements requested by the user.
    """

    STANDARD = 1
    """
    No special requests.
    """

    PROCESSING = 2
    """
    The user requests special processing such as a CPU count, removal of concurrency limits, etc.
    """

    BILLING = 3
    """
    The user requests that they bill another user.
    """


def _check_raise(name, value, source):
    raise IncorrectParamsException(
        f"Found illegal {name} '{value}' in job requirements from {source}"
    )


def _check_clientgroup(clientgroup, source):
    clientgroup = _string_request(clientgroup, "client group", source)
    # this is a possible error mode from the catalog since it uses key=value pairs in CSV
    # format
    if "=" in clientgroup:
        _check_raise("client group", clientgroup, source)
    return clientgroup


def _string_request(putative_string, name, source):
    if type(putative_string) != str:
        _check_raise(name, putative_string, source)
    return putative_string.strip()


def _int_request(putative_int, original, name, source):
    if type(putative_int) == float:
        _check_raise(f"{name} request", original, source)
    try:
        return int(putative_int)
    except (ValueError, TypeError):
        _check_raise(f"{name} request", original, source)


def _check_cpus(cpus, source):
    return _int_request(cpus, cpus, "cpu", source)


def _check_memory(memory, source):
    if type(memory) == int:
        return memory
    memory2 = _string_request(memory, "memory request", source)
    if memory2.endswith("M"):
        memory2 = memory2[:-1]
    elif memory2.endswith("MB"):
        memory2 = memory2[:-2]
    return _int_request(memory2, memory, "memory", source)


def _check_disk(disk, source):
    if type(disk) == int:
        return disk
    disk2 = _string_request(disk, "disk request", source)
    if disk2.endswith("GB"):
        disk2 = disk2[:-2]
    return _int_request(disk2, disk, "disk", source)


def _bool_request(putative_bool, name, source):
    if type(putative_bool) == bool or type(putative_bool) == int:
        return bool(putative_bool)
    pbs = _string_request(putative_bool, name, source).lower()
    if pbs == "true":
        return True
    if pbs == "false":
        return False
    _check_raise(name, putative_bool, source)


def _check_client_group_regex(client_group_regex, source) -> bool:
    return _bool_request(client_group_regex, "client group regex", source)


def _check_debug_mode(debug_mode, source) -> bool:
    return _bool_request(debug_mode, "debug mode", source)


_KEY_CHECKERS = {
    CLIENT_GROUP: _check_clientgroup,
    REQUEST_CPUS: _check_cpus,
    REQUEST_MEMORY: _check_memory,
    REQUEST_DISK: _check_disk,
    CLIENT_GROUP_REGEX: _check_client_group_regex,
    DEBUG_MODE: _check_debug_mode,
}


class JobRequirementsResolver:
    """
    Resolves requirements for a job (e.g. CPU, memory, etc.) given a method id and optional input
    parameters. Order of precedence is:
    1) Parameters submitted by the client programmer
    2) Requirements in the KBase Catalog service
    3) Requirements from the EE2 configuration file (deploy.cfg).
    """

    def __init__(
        self,
        catalog: Catalog,
        cfgfile: Iterable[str],
        override_client_group: str = None,
    ):
        """
        Create the resolver.

        catalog - a catalog client pointing at the relevant KBase catalog service.
        cfgfile - the configuration file as an open file object or other iterable.
        override_client_group - if provided, this client group will be used for all jobs, ignoring
            all other sources of client group information.
        """
        self._catalog = _not_falsy(catalog, "catalog")
        self._override_client_group = _check_string(
            override_client_group, "override_client_group", optional=True
        )
        config = ConfigParser()
        config.read_file(_not_falsy(cfgfile, "cfgfile"))
        self._default_client_group = _check_string(
            config.get(
                section=EE2_DEFAULT_SECTION,
                option=EE2_DEFAULT_CLIENT_GROUP,
                fallback=None,
            ),
            f"value for {EE2_DEFAULT_SECTION}.{EE2_DEFAULT_CLIENT_GROUP} in deployment config file",
        )
        self._clientgroup_default_configs = self._build_config(config)
        if self._default_client_group not in self._clientgroup_default_configs:
            raise ValueError(
                "No deployment configuration entry for default "
                + f"client group '{self._default_client_group}'"
            )
        if (
            self._override_client_group
            and self._override_client_group not in self._clientgroup_default_configs
        ):
            raise ValueError(
                "No deployment configuration entry for override "
                + f"client group '{self._override_client_group}'"
            )

    def _build_config(self, config):
        ret = {}
        for sec in config.sections():
            # if the default section is left as DEFAULT configparser shouldn't include it
            # in the list, but just in case it changes...
            if sec != EE2_CONFIG_SECTION and sec != EE2_DEFAULT_SECTION:
                reqspec = {item[0]: item[1] for item in config.items(sec)}
                reqspec[CLIENT_GROUP] = sec
                ret[sec] = self.normalize_job_reqs(
                    reqspec,
                    f"section '{sec}' of the deployment configuration",
                    require_all_resources=True,
                )
        return ret

    def get_override_client_group(self) -> Union[str, None]:
        """
        Get the override client group, if any. This client group supercedes all others.
        """
        return self._override_client_group

    def get_default_client_group(self) -> str:
        """
        Get the default client group used if a client group is not provided by override, the user,
        or the catalog.
        """
        return self._default_client_group

    def get_configured_client_groups(self) -> Set[str]:
        """
        Get the client groups configured in the configuration file.
        """
        return self._clientgroup_default_configs.keys()

    def get_configured_client_group_spec(
        self, clientgroup: str
    ) -> Dict[str, Union[int, str]]:
        f"""
        Get the client group specification in normalized format. Includes the {CLIENT_GROUP},
        {REQUEST_CPUS}, {REQUEST_MEMORY}, and {REQUEST_DISK} keys. May, but usually will not,
        include the {DEBUG_MODE} and {CLIENT_GROUP_REGEX} keys.
        """
        if clientgroup not in self._clientgroup_default_configs:
            raise ValueError(f"Client group '{clientgroup}' is not configured")
        # make a copy to prevent accidental mutation by the caller
        return dict(self._clientgroup_default_configs[clientgroup])

    @classmethod
    def get_requirements_type(
        self,
        cpus: int = None,
        memory_MB: int = None,
        disk_GB: int = None,
        client_group: str = None,
        client_group_regex: Union[bool, None] = None,
        bill_to_user: str = None,
        ignore_concurrency_limits: bool = False,
        scheduler_requirements: Dict[str, str] = None,
        debug_mode: bool = False,
    ) -> RequirementsType:
        f"""
        Determine what type of requirements are being requested.

        All parameters are optional.

        cpus - the number of CPUs required for the job.
        memory_MB - the amount of memory, in MB, required for the job.
        disk_GB - the amount of disk space, in GB, required for the job.
        client_group - the client group in which the job will run.
        client_group_regex - whether to treat the client group string as a regular expression
            that can match multiple client groups. Pass None for no preference.
        bill_to_user - bill the job to an alternate user; takes the user's username.
        ignore_concurrency_limits - allow the user to run this job even if the user's maximum
            job count has already been reached.
        scheduler_requirements - arbitrary requirements for the scheduler passed as key/value
            pairs. Requires knowledge of the scheduler API.
        debug_mode - whether to run the job in debug mode.

        Returns the type of requirements requested by the user:
        {RequirementsType.STANDARD.name} - if no requirements are requested
        {RequirementsType.PROCESSING.name} - if any requirements other than bill_to_user are
            requested
        {RequirementsType.BILLING.name} - if bill_to_user is requested
        """
        args = JobRequirements.check_parameters(
            cpus,
            memory_MB,
            disk_GB,
            client_group,
            client_group_regex,
            bill_to_user,
            ignore_concurrency_limits,
            scheduler_requirements,
            debug_mode,
        )
        if args[5]:  # bill_to_user
            return RequirementsType.BILLING
        if any(args) or args[4] is False:
            # regex False means the user is asking for non default
            return RequirementsType.PROCESSING
        return RequirementsType.STANDARD

    @classmethod
    def normalize_job_reqs(
        cls, reqs: Dict[str, Union[str, int]], source: str, require_all_resources=False
    ) -> Dict[str, Union[str, int]]:
        f"""
        Massage job requirements into a standard format. Does the following to specific keys of
        the reqs argument:

        {CLIENT_GROUP}: ensures it does not contain an =. This error mode is more probable in
            the KBase catalog UI.
        {REQUEST_CPUS}: parses to an int
        {REQUEST_MEMORY}: parses to an int, removing a trailing 'M' or 'MB' if necessary.
        {REQUEST_DISK}: parses to an int, removing a trailing 'GB' if necessary.
        {CLIENT_GROUP_REGEX}: parses to a boolean or None. The strings true and false are
            parsed to booleans, case-insensitive. Ints are parsed directly to booleans.
        {DEBUG_MODE}: parses to a boolean. The strings true and false are parsed to booleans,
            case-insensitive. Ints are parsed directly to booleans.

        reqs - the job requirements
        source - the source of the job requirements, e.g. catalog, user, etc.
        require_all_resources - True to throw an error if all four keys resources keys
            ({CLIENT_GROUP}, {REQUEST_CPUS}, {REQUEST_MEMORY}, {REQUEST_DISK}) aren't present
            with valid values.

        Returns a new dictionary with the altered keys. If any key is not present no action is
        taken for that key.
        """
        # TODO could support more units and convert as necessary (see checker funcs at start
        # of module). YAGNI for now.
        if reqs is None:
            reqs = {}
        ret = {}
        for key in [
            CLIENT_GROUP,
            REQUEST_CPUS,
            REQUEST_MEMORY,
            REQUEST_DISK,
            CLIENT_GROUP_REGEX,
            DEBUG_MODE,
        ]:
            if not cls._has_value(reqs.get(key)):
                if require_all_resources and key in _RESOURCES:
                    raise IncorrectParamsException(
                        f"Missing {key} key in job requirements from {source}"
                    )
            else:
                ret[key] = _KEY_CHECKERS[key](reqs.get(key), source)
        return ret

    @classmethod
    def _has_value(cls, inc):
        if inc is None:
            return False
        if type(inc) == str and not inc.strip():
            return False
        return True

    def resolve_requirements(
        self,
        method: str,
        catalog_cache: CatalogCache,
        cpus: int = None,
        memory_MB: int = None,
        disk_GB: int = None,
        client_group: str = None,
        client_group_regex: Union[bool, None] = None,
        bill_to_user: str = None,
        ignore_concurrency_limits: bool = False,
        scheduler_requirements: Dict[str, str] = None,
        debug_mode: bool = None,
    ) -> JobRequirements:
        """
        Resolve jobs requirements for a method.

        All parameters are optional other than the method and supplying them will override
        the catalog and ee2 settings for the job.

        method - the method to be run in module.method format.
        catalog_cache - a per request instance of a CatalogCache in order to speed up catalog lookups
        cpus - the number of CPUs required for the job.
        memory_MB - the amount of memory, in MB, required for the job.
        disk_GB - the amount of disk space, in GB, required for the job.
        client_group - the client group in which the job will run.
        client_group_regex - whether to treat the client group string as a regular expression
            that can match multiple client groups. Pass None for no preference.
        bill_to_user - bill the job to an alternate user; takes the user's username.
        ignore_concurrency_limits - allow the user to run this job even if the user's maximum
            job count has already been reached.
        scheduler_requirements - arbitrary requirements for the scheduler passed as key/value
            pairs. Requires knowledge of the scheduler API.
        debug_mode - whether to run the job in debug mode.

        Returns the job requirements.
        """

        if method is None or len(method.split(".")) != 2:
            raise IncorrectParamsException(
                f"Unrecognized method: '{method}'. Please input module_name.function_name"
            )
        module_name, function_name = [m.strip() for m in method.split(".")]

        args = JobRequirements.check_parameters(
            cpus,
            memory_MB,
            disk_GB,
            client_group,
            client_group_regex,
            bill_to_user,
            ignore_concurrency_limits,
            scheduler_requirements,
            debug_mode,
        )

        # the catalog could contain arbitrary scheduler requirements so we can't skip the
        # call even if all the arguments are provided
        cat_reqs_all = self._get_catalog_reqs(module_name, function_name, catalog_cache)
        cat_reqs = self.normalize_job_reqs(
            cat_reqs_all,
            f"catalog method {module_name}.{function_name}",
        )
        client_group = self._get_client_group(
            args[3], cat_reqs.get(CLIENT_GROUP), module_name, function_name
        )

        # don't mutate the spec, make a copy
        reqs = dict(self._clientgroup_default_configs[client_group])
        reqs.update(cat_reqs)

        scheduler_requirements = _remove_special_keys(cat_reqs_all)
        # don't mutate args, check_parameters doesn't make a copy of the incoming args
        scheduler_requirements.update(_remove_special_keys(dict(args[7])))

        cgr = args[4] if (args[4] is not None) else reqs.pop(CLIENT_GROUP_REGEX, None)
        dm = args[8] if (args[8] is not None) else reqs.pop(DEBUG_MODE, None)

        return JobRequirements(
            args[0] or reqs[REQUEST_CPUS],
            args[1] or reqs[REQUEST_MEMORY],
            args[2] or reqs[REQUEST_DISK],
            client_group,
            client_group_regex=cgr,
            bill_to_user=args[5],
            ignore_concurrency_limits=args[6],
            scheduler_requirements=scheduler_requirements,
            debug_mode=dm,
        )

    def _get_client_group(self, user_cg, catalog_cg, module_name, function_name):
        cg = next(
            i
            for i in [
                user_cg,
                self._override_client_group,
                catalog_cg,
                self._default_client_group,
            ]
            if i is not None
        )
        if cg not in self._clientgroup_default_configs:
            if cg == catalog_cg:
                raise IncorrectParamsException(
                    f"Catalog specified illegal client group '{cg}' for method "
                    + f"{module_name}.{function_name}"
                )
            raise IncorrectParamsException(f"No such clientgroup: {cg}")
        return cg

    @staticmethod
    def _get_catalog_reqs(
        module_name: str, function_name: str, catalog_cache: CatalogCache
    ):
        # could cache results for 30s or so to speed things up... YAGNI
        group_config = catalog_cache.lookup_job_resource_requirements(
            module_name=module_name, function_name=function_name
        )
        # If group_config is empty, that means there's no clientgroup entry in the catalog
        # It'll return an empty list even for non-existent modules
        if not group_config:
            return {}
        if len(group_config) > 1:
            raise ValueError(
                "Unexpected result from the Catalog service: more than one client group "
                + f"configuration found for method {module_name}.{function_name} {group_config}"
            )

        resources_request = group_config[0].get(_CLIENT_GROUPS, None)

        # No client group provided
        if not resources_request:
            return {}
        # JSON
        if "{" in resources_request[0]:
            try:
                rv = json.loads(", ".join(resources_request))
            except ValueError:
                raise ValueError(
                    "Unable to parse JSON client group entry from catalog "
                    + f"for method {module_name}.{function_name}"
                )
            return {k.strip(): rv[k] for k in rv}
        # CSV Format
        # This presents as CSV in the Catalog UI, e.g.
        # clientgroup, key1=value1, key2=value2
        # and so on
        # The UI splits by comma before sending the data to the catalog, which is what we
        # get when we pull the data
        rv = {CLIENT_GROUP: resources_request.pop(0)}
        for item in resources_request:
            if "=" not in item:
                raise ValueError(
                    f"Malformed requirement. Format is <key>=<value>. Item is '{item}' for "
                    + f"catalog method {module_name}.{function_name}"
                )
            (key, value) = item.split("=", 1)
            rv[key.strip()] = value.strip()
        return rv
