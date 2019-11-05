class IncorrectParamsException(ValueError):
    pass


class MissingRunJobParamsException(ValueError):
    pass


class InvalidStatusTransitionException(ValueError):
    pass


class MissingCondorRequirementsException(ValueError):
    pass


class MalformedJobIdException(Exception):
    pass


class MalformedTimestampException(Exception):
    pass


class RecordNotFoundException(Exception):
    pass


class AuthError(Exception):
    """Raised if a user is unauthorized for a particular action, or doesn't have the right auth role"""

    pass
