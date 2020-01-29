import json


def normalize_catalog_cgroups(resources_request):
    """
    Ensure that the client_groups are processed as a dictionary and has at least one value
    :param resources_request: either an empty string, a json object, or cg,key1=value,key2=value
    :return:
    """
    if type(resources_request) is not str:
        raise TypeError(str(type(resources_request)))

    # No clientgroup provided
    if resources_request == "":
        return {}
    # JSON
    if "{" in resources_request:
        return json.loads(resources_request)

    rr = resources_request.split(",")
    # Default

    rv = {"client_group": rr.pop(0), "client_group_regex": True}
    for item in rr:
        if "=" not in item:
            raise Exception(
                f"Malformed requirement. Format is <key>=<value> . Item is {item}"
            )
        (key, value) = item.split("=")
        rv[key] = value

        if key in ["client_group_regex"]:
            raise ValueError(
                "Illegal argument! Old format does not support this option ('client_group_regex')"
            )

    return rv
