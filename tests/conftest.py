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
