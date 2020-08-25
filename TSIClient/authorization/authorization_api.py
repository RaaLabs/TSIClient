import requests
import logging
import json


class AuthorizationApi:
    def __init__(self, client_id, client_secret, tenant_id, api_version):
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id


    def _getToken(self):
        """Gets an authorization token from the Azure TSI api which is used to authenticate api calls.

        Returns:
            str: The authorization token.
        """

        url = "https://login.microsoftonline.com/{0!s}/oauth2/token".format(
            self._tenant_id
        )

        payload = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "resource": "https%3A%2F%2Fapi.timeseries.azure.com%2F&undefined=",
        }

        payload = "grant_type={grant_type}&client_id={client_id}&client_secret={client_secret}&resource={resource}".format(
            **payload
        )

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "cache-control": "no-cache",
        }

        try:
            response = requests.request(
                "POST", url, data=payload, headers=headers, timeout=10
            )
            response.raise_for_status()
        except requests.exceptions.ConnectTimeout:
            logging.error("TSIClient: The request to the TSI api timed out.")
            raise
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            if status_code == 401:
                logging.error(
                    "TSIClient: Authentication with the TSI api was unsuccessful. Check your client secret."
                )
            else:
                logging.error(
                    "TSIClient: The request to the TSI api returned an unsuccessfull status code. Check the stack trace"
                )
            raise

        jsonResp = json.loads(response.text)
        tokenType = jsonResp["token_type"]
        authorizationToken = tokenType + " " + jsonResp["access_token"]

        return authorizationToken
        