import json


def normalize_catalog_cgroups(resources_request):
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

    rv = {"client_group": resources_request.pop(0)}
    for item in resources_request:
        if "=" not in item:
            raise Exception(
                f"Malformed requirement. Format is <key>=<value> . Item is {item}"
            )
        (key, value) = item.split("=")
        rv[key] = value

    return rv
