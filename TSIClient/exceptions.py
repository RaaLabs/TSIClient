class TSIClientError(Exception):
    """Base class for all exceptions in the TSIClient SDK.
    """

    pass


class TSIEnvironmentError(TSIClientError):
    """TSI environment error. Raised if no TSI environment is found.
    """

    def __init__(self, message=None):
        self.message = message
