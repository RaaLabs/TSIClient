class TSIClientError(Exception):
    """Base class for all exceptions in the TSIClient SDK.
    """


class TSIEnvironmentError(TSIClientError):
    """TSI environment error. Raised if no TSI environment is found.
    """

    def __init__(self, message=None):
        self.message = message

class TSIQueryError(TSIClientError):
    """TSI query error. Raised if there was a problem with the TSI query, usually wrong input format of query arguments.
    """

    def __init__(self, message=None):
        self.message = message

class TSIStoreError(TSIClientError):
    """TSI store error. Raised if warm store is not enabled, but query was tried to execute on warm store.
    """

    def __init__(self, message=None):
        self.message = message
