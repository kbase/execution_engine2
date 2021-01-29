"""
Functions for processing arguments / parameters, including argument validity checkers and
normalizers.
"""

from typing import Union


def parse_bool(putative_bool: Union[str, bool, int, None]) -> bool:
    """
    Parse a string, bool, or int to a boolean value.
    Strings containing 'true' or 'false', regardless of capitalization, are considered booleans.

    Raises ValueError if the value cannot be parsed.
    """
    if putative_bool is None:
        return False

    if isinstance(putative_bool, bool):
        return putative_bool

    if isinstance(putative_bool, int):
        return bool(putative_bool)

    if isinstance(putative_bool, str):
        if putative_bool.lower() == "true":
            return True
        if putative_bool.lower() == "false":
            return False

    raise ValueError(f"{putative_bool} is not a boolean value")
