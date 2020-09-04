import os
import pytest
from TSIClient import TSIClient as tsi
from tests.mock_responses import MockURLs, MockResponses


@pytest.fixture
def client(requests_mock):
    requests_mock.request(
        "POST",
        MockURLs.oauth_url,
        json=MockResponses.mock_oauth
    )
    requests_mock.request(
        "GET",
        MockURLs.env_url,
        json=MockResponses.mock_environments
    )
    requests_mock.request(
        "GET",
        MockURLs.instances_url,
        json=MockResponses.mock_instances
    )
    client = tsi.TSIClient(
        environment='Test_Environment',
        client_id="MyClientID",
        client_secret="a_very_secret_password",
        applicationName="postmanServicePrincipal",
        tenant_id="yet_another_tenant_id"
    )

    return client

@pytest.fixture
def client_from_env(requests_mock):
    os.environ["TSICLIENT_APPLICATION_NAME"] = "my_app"
    os.environ["TSICLIENT_ENVIRONMENT_NAME"] = "Test_Environment"
    os.environ["TSICLIENT_CLIENT_ID"] = "my_client_id"
    os.environ["TSICLIENT_CLIENT_SECRET"] = "my_client_secret"
    os.environ["TSICLIENT_TENANT_ID"] = "yet_another_tenant_id"

    requests_mock.request(
        "POST",
        MockURLs.oauth_url,
        json=MockResponses.mock_oauth
    )
    requests_mock.request(
        "GET",
        MockURLs.env_url,
        json=MockResponses.mock_environments
    )
    requests_mock.request(
        "GET",
        MockURLs.instances_url,
        json=MockResponses.mock_instances
    )

    return tsi.TSIClient()