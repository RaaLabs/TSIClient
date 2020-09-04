import requests
import pytest
from tests.mock_responses import MockURLs, MockResponses


class TestHierarchiesApi:
    def test_getHierarchies_success(self, client, requests_mock):
        requests_mock.request(
            "GET", MockURLs.hierarchies_url, json=MockResponses.mock_hierarchies
        )

        resp = client.hierarchies.getHierarchies()

        assert len(resp["hierarchies"]) == 1
        assert isinstance(resp["hierarchies"], list)
        assert isinstance(resp["hierarchies"][0], dict)
        assert resp["hierarchies"][0]["id"] == "6e292e54-9a26-4be1-9034-607d71492707"

    def test_getHierarchies_raises_HTTPError(self, client, requests_mock, caplog):
        requests_mock.request(
            "GET", MockURLs.hierarchies_url, exc=requests.exceptions.HTTPError
        )

        with pytest.raises(requests.exceptions.HTTPError):
            client.hierarchies.getHierarchies()

        assert (
            "TSIClient: The request to the TSI api returned an unsuccessfull status code."
            in caplog.text
        )

    def test_getHierarchies_raises_ConnectTimeout(self, client, requests_mock, caplog):
        requests_mock.request(
            "GET", MockURLs.hierarchies_url, exc=requests.exceptions.ConnectTimeout
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.hierarchies.getHierarchies()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text
