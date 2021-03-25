"""
Contains resolvers for job requirements.
"""

from typing import Dict, Union

from execution_engine2.sdk.job_submission_parameters import JobRequirements
from execution_engine2.exceptions import IncorrectParamsException

CLIENT_GROUP = "client_group"
REQUEST_CPUS = "request_cpus"
REQUEST_MEMORY = "request_memory"
REQUEST_DISK = "request_disk"
CLIENT_GROUP_REGEX = "client_group_regex"
DEBUG_MODE = "debug_mode"
_RESOURCES = set([CLIENT_GROUP, REQUEST_CPUS, REQUEST_MEMORY, REQUEST_DISK])


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
    except ValueError:
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

    @classmethod
    def normalize_job_reqs(
        cls, reqs: Dict[str, str], source: str, require_all_resources=False
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
