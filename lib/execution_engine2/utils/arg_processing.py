"""
Functions for processing arguments / parameters, including argument validity checkers and
normalizers.
"""

from typing import Union


def parse_bool(putative_bool: Union[str, bool, int, float, None]) -> bool:
    """
    Parse a string, bool, int, or float to a boolean value.
    Strings containing 'true' or 'false', regardless of capitalization, are considered booleans.
    Strings containing ints or floats are parsed to floats before processing.

    Raises ValueError if the value cannot be parsed.
    """
    pb = putative_bool
    if pb is None:
        return False

    if isinstance(pb, bool) or isinstance(pb, int) or isinstance(pb, float):
        return bool(pb)

    if isinstance(pb, str):
        try:
            return bool(float(pb))
        except ValueError:
            pass  # check for 'true' and 'false' strings next
        # they're more likely and if we really wanted to optimize they should go first.
        # probably doesn't matter at all and it makes the code a bit simpler
        if pb.lower() == "true":
            return True
        if pb.lower() == "false":
            return False

    raise ValueError(f"{pb} is not a boolean value")
