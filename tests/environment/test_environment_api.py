import pytest
import requests
from TSIClient.exceptions import TSIEnvironmentError
from tests.mock_responses import MockURLs, MockResponses


class TestEnvironmentApi:
    def test_getEnvironment_success(self, client):
        env_id = client.environment.getEnvironmentId()

        assert env_id == "00000000-0000-0000-0000-000000000000"

    def test_getEnvironment_raises_HTTPError(self, client, requests_mock, caplog):
        requests_mock.request(
            "GET", MockURLs.env_url, exc=requests.exceptions.HTTPError
        )

        with pytest.raises(requests.exceptions.HTTPError):
            client.environment.getEnvironmentId()

        assert (
            "TSIClient: The request to the TSI api returned an unsuccessfull status code."
            in caplog.text
        )

    def test_getEnvironment_raises_ConnectTimeout(self, client, requests_mock, caplog):
        requests_mock.request(
            "GET", MockURLs.env_url, exc=requests.exceptions.ConnectTimeout
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.environment.getEnvironmentId()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text

    def test_getEnvironments_raises_TSIEnvironmentError(self, client, requests_mock):
        requests_mock.request(
            "GET",
            MockURLs.env_url,
            exc=TSIEnvironmentError(
                "Azure TSI environment not found. Check the spelling or create an environment in Azure TSI."
            ),
        )

        with pytest.raises(TSIEnvironmentError) as exc_info:
            client.environment.getEnvironmentId()

        assert (
            "Azure TSI environment not found. Check the spelling or create an environment in Azure TSI."
            in str(exc_info.value)
        )

    def test_getEnvironmentAvailability_success(self, client, requests_mock):
        requests_mock.request(
            "GET",
            MockURLs.environment_availability_url,
            json=MockResponses.mock_environment_availability,
        )

        resp = client.environment.getEnvironmentAvailability()

        assert isinstance(resp["availability"], dict)
        assert "intervalSize" in resp["availability"]
        assert "distribution" in resp["availability"]
        assert "range" in resp["availability"]

    def test_getEnvironmentAvailability_raises_HTTPError(
        self, client, requests_mock, caplog
    ):
        requests_mock.request(
            "GET",
            MockURLs.environment_availability_url,
            exc=requests.exceptions.HTTPError,
        )

        with pytest.raises(requests.exceptions.HTTPError):
            client.environment.getEnvironmentAvailability()

        assert (
            "TSIClient: The request to the TSI api returned an unsuccessfull status code."
            in caplog.text
        )

    def test_getEnvironmentAvailability_raises_ConnectTimeout(
        self, client, requests_mock, caplog
    ):
        requests_mock.request(
            "GET",
            MockURLs.environment_availability_url,
            exc=requests.exceptions.ConnectTimeout,
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.environment.getEnvironmentAvailability()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text
