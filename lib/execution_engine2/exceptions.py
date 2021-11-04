class ExecutionEngineValueError(ValueError):
    """
    Base Class for ee2 value exceptions
    Subclass exceptions use docstring as default message
    """

    def __init__(self, msg=None, *args, **kwargs):
        super().__init__(msg or self.__doc__, *args, **kwargs)


class ExecutionEngineException(Exception):
    """
    Base Class for ee2 exceptions
    Subclass exceptions use docstring as default message
    """

    def __init__(self, msg=None, *args, **kwargs):
        super().__init__(msg or self.__doc__, *args, **kwargs)


class InvalidStatusListException(ExecutionEngineValueError):
    """Invalid job status provided"""


class BatchTerminationException(ExecutionEngineException):
    """No jobs to terminate"""


class IncorrectParamsException(ExecutionEngineValueError):
    """Wrong parameters were provided"""


class NotBatchJobException(ExecutionEngineValueError):
    """Requested job is not a batch job"""


class InvalidParameterForBatch(ExecutionEngineValueError):
    """Workspace ids are not allowed in RunJobParams in Batch Mode"""


class MissingRunJobParamsException(ExecutionEngineValueError):
    """Provided an empty (RunJobParams) parameter mapping"""


class InvalidStatusTransitionException(ExecutionEngineValueError):
    """Raised if the status transition is NOT ALLOWED"""


class InvalidOperationForStatusException(ExecutionEngineValueError):
    """The current operation is not valid for this job status"""


class MissingCondorRequirementsException(ExecutionEngineValueError):
    """Raised if malformed requirements information is retrieved for an ee2 job"""


class MalformedJobIdException(ExecutionEngineValueError):
    """Raised if bad ee2 id is passed in"""


class MalformedTimestampException(ExecutionEngineException):
    """Bad timestamps"""


class ChildrenNotFoundError(ExecutionEngineException):
    """Raised if children are not found for a given parent when attempting to abandon children"""


class RecordNotFoundException(ExecutionEngineException):
    """Raised if ee2 job or ee2 job log record is not found in db"""


class CondorJobNotFoundException(ExecutionEngineException):
    """Raised if condor job is not found"""


class RetryFailureException(ExecutionEngineException):
    """General exception for couldn't Retry the job failures'"""


class CannotRetryJob(ExecutionEngineException):
    """Can only retry errored or cancelled jobs, and not batch parents"""


class AuthError(ExecutionEngineException):
    """Raised if a user is unauthorized for a particular action, or doesn't have the right auth role"""
