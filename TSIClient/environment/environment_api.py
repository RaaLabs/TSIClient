import requests
import logging
import json
from ..authorization.authorization_api import AuthorizationApi
from ..exceptions import TSIEnvironmentError
from ..common.common_funcs import CommonFuncs


class EnvironmentApi:
    def __init__(
        self,
        application_name: str,
        environment: str,
        authorization_api: AuthorizationApi,
        common_funcs: CommonFuncs,
    ):
        self._applicationName = application_name
        self._enviromentName = environment
        self.authorization_api = authorization_api
        self.common_funcs = common_funcs


    def getEnviroment(self):
        """Gets the id of the environment specified in the TSIClient class constructor.

        Returns:
            str: The environment id.

        Raises:
            TSIEnvironmentError: Raised if the TSI environment does not exist.

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> env = client.getEnviroment()
        """

        authorizationToken = self.authorization_api._getToken()
        url = "https://api.timeseries.azure.com/environments"

        querystring = self.common_funcs._getQueryString()

        payload = ""
        headers = {
            "x-ms-client-application-name": self._applicationName,
            "Authorization": authorizationToken,
            "Content-Type": "application/json",
            "cache-control": "no-cache",
        }

        try:
            response = requests.request(
                "GET",
                url,
                data=payload,
                headers=headers,
                params=querystring,
                timeout=10,
            )
            response.raise_for_status()
        except requests.exceptions.ConnectTimeout:
            logging.error("TSIClient: The request to the TSI api timed out.")
            raise
        except requests.exceptions.HTTPError:
            logging.error(
                "TSIClient: The request to the TSI api returned an unsuccessfull status code."
            )
            raise

        environments = json.loads(response.text)["environments"]
        environmentId = None
        for enviroment in environments:
            if enviroment["displayName"] == self._enviromentName:
                environmentId = enviroment["environmentId"]
                break
        if environmentId == None:
            raise TSIEnvironmentError(
                "TSIClient: TSI environment not found. Check the spelling or create an environment in Azure TSI."
            )

        return environmentId
