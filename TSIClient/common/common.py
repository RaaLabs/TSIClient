class CommonFuncs:
    def __init__(self, api_version):
        self.api_version = api_version


    def _getQueryString(self, useWarmStore=None):
        """Creates the querystring for an api request.
        
        Can be used in all api requests in TSIClient.

        Args:
            useWarmStore (bool): A boolean to indicate the storeType. Defaults to None,
                in which case no storeType param is included in the querystring.

        Returns:
            dict: The querystring with the api-version and optionally the storeType.
        """

        if useWarmStore == None:
            return {"api-version": self.api_version}

        else:
            return {
                "api-version": self.api_version,
                "storeType": "WarmStore" if useWarmStore == True else "ColdStore",
            }
