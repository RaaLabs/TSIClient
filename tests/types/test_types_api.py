import requests
import pytest
from tests.mock_responses import MockURLs, MockResponses


class TestTypes:
    def test_getTypes_success(self, client, requests_mock):
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        resp = client.types.getTypes()

        assert len(resp["types"]) == 2
        assert isinstance(resp["types"], list)
        assert isinstance(resp["types"][0], dict)
        assert resp["types"][0]["id"] == "1be09af9-f089-4d6b-9f0b-48018b5f7393"

    def test_getTypes_raises_HTTPError(self, client, requests_mock, caplog):
        requests_mock.request(
            "GET", MockURLs.types_url, exc=requests.exceptions.HTTPError
        )

        with pytest.raises(requests.exceptions.HTTPError):
            client.types.getTypes()

        assert (
            "TSIClient: The request to the TSI api returned an unsuccessfull status code."
            in caplog.text
        )

    def test_getTypes_raises_ConnectTimeout(self, client, requests_mock, caplog):
        requests_mock.request(
            "GET", MockURLs.types_url, exc=requests.exceptions.ConnectTimeout
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.types.getTypes()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text
