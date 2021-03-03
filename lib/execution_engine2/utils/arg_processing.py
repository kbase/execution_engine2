"""
Functions for processing arguments / parameters, including argument validity checkers and
normalizers.
"""

from typing import Optional, Iterable, TypeVar, Union
import datetime
import unicodedata
from execution_engine2.exceptions import IncorrectParamsException

T = TypeVar('T')


def parse_bool(putative_bool: Union[str, bool, int, float, None]) -> bool:
    """
    Parse a string, bool, int, or float to a boolean value.
    Strings containing 'true' or 'false', regardless of capitalization, are considered booleans.
    Strings containing ints or floats are parsed to floats before processing.

    Raises IncorrectParamsException if the value cannot be parsed.
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

    raise IncorrectParamsException(f"{pb} is not a boolean value")


# The remaining methods are ported from
# https://github.com/kbase/sample_service/blob/master/lib/SampleService/core/arg_checkers.py
# with slight changes.
# Should probably make a package or see if there are equivalent 3rd party functions at some point.
# Although if you want to use custom exceptions as here that won't work


def not_falsy(item: T, item_name: str) -> T:
    '''
    Check if a value is falsy and throw and exception if so.
    :param item: the item to check for falsiness.
    :param item_name: the name of the item to include in any exception.
    :raises ValueError: if the item is falsy.
    :returns: the item.
    '''
    if not item:
        raise ValueError(f'{item_name} cannot be a value that evaluates to false')
    return item


def not_falsy_in_iterable(
        iterable: Optional[Iterable[T]],
        name: str,
        allow_none: bool = False) -> Optional[Iterable[T]]:
    '''
    Check that an iterable is not None and contains no falsy items. Empty iterables are accepted.
    :param iterable: the iterable to check.
    :param name: the name of the iterable to be used in error messages.
    :param allow_none: allow the iterable to be None - in this case return None. The contents of
        the iterable may not be None.
    :returns: the iterable.
    :raises ValueError: if the iterable is None or contains falsy items.
    '''
    # probably need to allow for 0 as an option
    if iterable is None:
        if allow_none:
            return None
        raise ValueError(f'{name} cannot be None')
    for i, item in enumerate(iterable):
        not_falsy(item, f'Index {i} of iterable {name}')
    return iterable


def _contains_control_characters(string: str) -> bool:
    '''
    Check if a string contains control characters, as denoted by the Unicode character category
    starting with a C.
    :param string: the string to check.
    :returns: True if the string contains control characters, False otherwise.
    '''
    # make public if needed
    # See https://stackoverflow.com/questions/4324790/removing-control-characters-from-a-string-in-python  # noqa: E501
    for c in string:
        if unicodedata.category(c)[0] == 'C':
            return True
    return False


def _no_control_characters(string: str, name: str) -> str:
    '''
    Checks that a string contains no control characters and throws an exception if it does.
    See :meth:`contains_control_characters` for more information.
    :param string: The string to check.
    :param name: the name of the string to include in any exception.
    :raises IncorrectParamsException: if the string contains control characters.
    :returns: the string.
    '''
    # make public if needed
    if _contains_control_characters(string):
        raise IncorrectParamsException(name + ' contains control characters')
    return string


def check_string(string: Optional[str], name: str, max_len: int = None, optional: bool = False
                 ) -> Optional[str]:
    '''
    Check that a string meets a set of criteria:
    - it is not None or whitespace only (unless the optional parameter is specified)
    - it contains no control characters
    - (optional) it is less than some specified maximum length
    :param string: the string to test.
    :param name: the name of the string to be used in error messages.
    :param max_len: the maximum length of the string.
    :param optional: True if no error should be thrown if the string is None.
    :returns: the stripped string or None if the string was optional and None or whitespace only.
    :raises IncorrectParamsException: if the string is None, whitespace only, too long, or
        contains illegal characters.
    '''
    # See the IDMapping service if character classes are needed.
    # Maybe package this stuff
    if max_len is not None and max_len < 1:
        raise ValueError('max_len must be > 0 if provided')
    if not string or not string.strip():
        if optional:
            return None
        raise IncorrectParamsException("Missing input parameter: " + name)
    string = string.strip()
    _no_control_characters(string, name)
    if max_len and len(string) > max_len:
        raise IncorrectParamsException(f'{name} exceeds maximum length of {max_len}')
    return string


def check_timestamp(timestamp: datetime.datetime, name: str):
    '''
    Check that a timestamp is not None and not naive. See
    https://docs.python.org/3.8/library/datetime.html#aware-and-naive-objects
    :param timestamp: the timestamp to check.
    :param name: the name of the variable to use in thrown errors.
    :returns: the timestamp.
    :raises ValueError: if the check fails.
    '''
    if not_falsy(timestamp, name).tzinfo is None:
        # The docs say you should also check savetime.tzinfo.utcoffset(savetime) is not None,
        # but initializing a datetime with a tzinfo subclass that returns None for that method
        # causes the constructor to throw an error
        raise ValueError(f'{name} cannot be a naive datetime')
    return timestamp
