import os
import pytest
from TSIClient import TSIClient as tsi


@pytest.fixture(scope="module")
def client():
    client = tsi.TSIClient(
        enviroment='Test_Environment',
        client_id="MyClientID",
        client_secret="a_very_secret_password",
        applicationName="postmanServicePrincipal",
        tenant_id="yet_another_tenant_id"
    )

    return client


@pytest.fixture(scope="module")
def client_from_env():
    os.environ["TSICLIENT_APPLICATION_NAME"] = "my_app"
    os.environ["TSICLIENT_ENVIRONMENT_NAME"] = "my_environment"
    os.environ["TSICLIENT_CLIENT_ID"] = "my_client_id"
    os.environ["TSICLIENT_CLIENT_SECRET"] = "my_client_secret"
    os.environ["TSICLIENT_TENANT_ID"] = "my_tenant_id"

    return tsi.TSIClient()
