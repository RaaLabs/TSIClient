class TSIClientError(Exception):
    """Base class for all exceptions in the TSIClient SDK.
    """

    pass


class TSIEnvironmentError(TSIClientError):
    """TSI environment error. Raised if no TSI environment is found.
    """

    def __init__(self, message=None):
        self.message = message


class TSIStoreError(TSIClientError):
    """TSI store error. Raised if warm store is not enabled, but query was tried to execute on warm store.
    """

    def __init__(self, message=None):
        self.message = message
