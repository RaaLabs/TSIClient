import requests
import pytest
from collections import namedtuple
from tests.mock_responses import MockURLs, MockResponses


class TestAuthorizationApi():
    def test__getToken_success(self, client):
        token = client.authorization._getToken()
        assert token == "some_type token"


    def test__getToken_raises_401_HTTPError(self, client, requests_mock, caplog):
        httperror_response = namedtuple("httperror_response", "status_code")
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            exc=requests.exceptions.HTTPError(response=httperror_response(status_code=401))
        )

        with pytest.raises(requests.exceptions.HTTPError):
            client.authorization._getToken()

        assert "TSIClient: Authentication with the TSI api was unsuccessful. Check your client secret." in caplog.text


    def test__getToken_raises_ConnectTimeout(self, client,requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            exc=requests.exceptions.ConnectTimeout
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.authorization._getToken()
