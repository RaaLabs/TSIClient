import os


class TestTSIClient:
    def test_create_TSICLient_success(self, client):
        assert client._applicationName == "postmanServicePrincipal"
        assert client._environmentName == "Test_Environment"
        assert client._client_id == "MyClientID"
        assert client._client_secret == "a_very_secret_password"
        assert client._tenant_id == "yet_another_tenant_id"
        assert client._apiVersion == "2020-07-31"

    def test_create_TSICLient_with_api_version_success(self, client):
        assert client._applicationName == "postmanServicePrincipal"
        assert client._environmentName == "Test_Environment"
        assert client._client_id == "MyClientID"
        assert client._client_secret == "a_very_secret_password"
        assert client._tenant_id == "yet_another_tenant_id"
        assert client._apiVersion == "2020-07-31"

    def test_create_TSIClient_from_env(self, client_from_env):
        assert client_from_env._applicationName == "my_app"
        assert client_from_env._environmentName == "Test_Environment"
        assert client_from_env._client_id == "my_client_id"
        assert client_from_env._client_secret == "my_client_secret"
        assert client_from_env._tenant_id == "yet_another_tenant_id"
        assert client_from_env._apiVersion == "2020-07-31"

    def test_create_TSIClient_from_env_with_api_version(self, client_from_env):
        os.environ["TSICLIENT_APPLICATION_NAME"] = "my_app"
        os.environ["TSICLIENT_ENVIRONMENT_NAME"] = "Test_Environment"
        os.environ["TSICLIENT_CLIENT_ID"] = "my_client_id"
        os.environ["TSICLIENT_CLIENT_SECRET"] = "my_client_secret"
        os.environ["TSICLIENT_TENANT_ID"] = "yet_another_tenant_id"
        os.environ["TSI_API_VERSION"] = "2020-07-31"

        assert client_from_env._applicationName == "my_app"
        assert client_from_env._environmentName == "Test_Environment"
        assert client_from_env._client_id == "my_client_id"
        assert client_from_env._client_secret == "my_client_secret"
        assert client_from_env._tenant_id == "yet_another_tenant_id"
        assert client_from_env._apiVersion == "2020-07-31"
