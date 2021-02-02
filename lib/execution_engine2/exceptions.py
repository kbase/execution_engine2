class ExecutionEngineValueError(ValueError):
    """Base Class for ee2 exceptions"""

    pass


class ExecutionEngineException(Exception):
    pass


class IncorrectParamsException(ExecutionEngineValueError):
    pass


class MissingRunJobParamsException(ExecutionEngineValueError):
    """ Raised if job is missing RunJobParams"""

    pass


class InvalidStatusTransitionException(ExecutionEngineValueError):
    """ Raised if the status transition is NOT ALLOWED"""

    pass


class InvalidOperationForStatusException(ExecutionEngineValueError):
    pass


class MissingCondorRequirementsException(ExecutionEngineValueError):
    """ Raised if malformed requirements information is retrieved for an ee2 job"""

    pass


class MalformedJobIdException(ExecutionEngineValueError):
    """ Raised if bad ee2 id is passed in"""

    pass


class MalformedTimestampException(ExecutionEngineException):
    """ Bad timestamps """

    pass


class ChildrenNotFoundError(ExecutionEngineException):
    """Raised if children are not found for a given parent when attempting to abandon children"""

    pass


class RecordNotFoundException(ExecutionEngineException):
    """Raised if ee2 job or ee2 job log record is not found in db"""

    pass


class CondorJobNotFoundException(ExecutionEngineException):
    """Raised if condor job is not found"""

    pass


class CondorFailedJobSubmit(ExecutionEngineException):
    """Raised upon failure of submission to the HTCondor Queue"""

    pass


class MultipleParentJobsException(ExecutionEngineException):
    """Raised if multiple parent jobs are submitted in the same batch job"""

    pass


class AuthError(ExecutionEngineException):
    """Raised if a user is unauthorized for a particular action, or doesn't have the right auth role"""

    pass
