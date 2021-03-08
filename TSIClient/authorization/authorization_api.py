import requests
import logging
import json
from azure.identity import DefaultAzureCredential


class AuthorizationApi:
    def __init__(self):
        self.credentials = DefaultAzureCredential(exclude_shared_token_cache_credential=True)


    def _getToken(self):
        azure_token_object = self.credentials.get_token("https://api.timeseries.azure.com/")

        return f"Bearer {azure_token_object.token}"
