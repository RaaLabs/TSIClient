import pytest
import requests
import pandas as pd
from collections import namedtuple
from TSIClient.exceptions import TSIEnvironmentError, TSIStoreError, TSIQueryError
from tests.mock_responses import MockURLs, MockResponses


class TestTSIClient():
    def test_create_TSICLient_success(self, client):
        assert client._applicationName == "postmanServicePrincipal"
        assert client._enviromentName == "Test_Environment"
        assert client._client_id == "MyClientID"
        assert client._client_secret == "a_very_secret_password"
        assert client._tenant_id == "yet_another_tenant_id"


    def test_instantiate_TSIClient_from_env(self, client_from_env):
        assert client_from_env._applicationName == "my_app"
        assert client_from_env._enviromentName == "my_environment"
        assert client_from_env._client_id == "my_client_id"
        assert client_from_env._client_secret == "my_client_secret"
        assert client_from_env._tenant_id == "my_tenant_id"


    def test__getToken_success(self, requests_mock, client):
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            json=MockResponses.mock_oauth
        )

        token = client._getToken()

        assert token == "some_type token"


    def test__getToken_raises_401_HTTPError(self, requests_mock, caplog, client):
        httperror_response = namedtuple("httperror_response", "status_code")
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            exc=requests.exceptions.HTTPError(response=httperror_response(status_code=401))
        )

        with pytest.raises(requests.exceptions.HTTPError):
            client._getToken()

        assert "TSIClient: Authentication with the TSI api was unsuccessful. Check your client secret." in caplog.text


    def test__getToken_raises_ConnectTimeout(self, requests_mock, client):
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            exc=requests.exceptions.ConnectTimeout
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client._getToken()


    def test__getVariableAggregate(self, requests_mock, client):
        aggregate, requestType = client._getVariableAggregate(aggregate=None)
        
        assert aggregate == None
        assert requestType == "getSeries"


    def test__getVariableAggregate(self, requests_mock, client, caplog):
        with pytest.raises(TSIQueryError):
            client._getVariableAggregate(aggregate="unsupported_aggregate")


    def test__getVariableAggregate(self, requests_mock, client):
        aggregate, requestType = client._getVariableAggregate(aggregate="avg")
        
        assert aggregate == {'tsx': 'avg($value)'}
        assert isinstance(aggregate, dict)
        assert requestType == "aggregateSeries"


    def test_getEnvironment_success(self, requests_mock, client):
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

        env_id = client.getEnviroment()

        assert env_id == "00000000-0000-0000-0000-000000000000"


    def test_getEnvironment_raises_HTTPError(self, requests_mock, caplog, client):
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            json=MockResponses.mock_oauth
        )
        requests_mock.request(
            "GET",
            MockURLs.env_url,
            exc=requests.exceptions.HTTPError
        )

        with pytest.raises(requests.exceptions.HTTPError):
            client.getEnviroment()

        assert "TSIClient: The request to the TSI api returned an unsuccessfull status code." in caplog.text


    def test_getEnvironment_raises_ConnectTimeout(self, requests_mock, caplog, client):
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            json=MockResponses.mock_oauth
        )
        requests_mock.request(
            "GET",
            MockURLs.env_url,
            exc=requests.exceptions.ConnectTimeout
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.getEnviroment()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text


    def test_getEnvironments_raises_TSIEnvironmentError(self, requests_mock, client):
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            json=MockResponses.mock_oauth
        )
        requests_mock.request(
            "GET",
            MockURLs.env_url,
            exc=TSIEnvironmentError("Azure TSI environment not found. Check the spelling or create an environment in Azure TSI.")
        )

        with pytest.raises(TSIEnvironmentError) as exc_info:
            client.getEnviroment()

        assert "Azure TSI environment not found. Check the spelling or create an environment in Azure TSI." in str(exc_info.value)


    def test_getEnvironmentAvailability_success(self, requests_mock, client):
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
            MockURLs.environment_availability_url,
            json=MockResponses.mock_environment_availability
        )

        resp = client.getEnvironmentAvailability()

        assert isinstance(resp["availability"], dict)
        assert "intervalSize" in resp["availability"]
        assert "distribution" in resp["availability"]
        assert "range" in resp["availability"]


    def test_getEnvironmentAvailability_raises_HTTPError(self, requests_mock, caplog, client):
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
            MockURLs.environment_availability_url,
            exc=requests.exceptions.HTTPError
        )

        with pytest.raises(requests.exceptions.HTTPError):
            client.getEnvironmentAvailability()

        assert "TSIClient: The request to the TSI api returned an unsuccessfull status code." in caplog.text


    def test_getEnvironmentAvailability_raises_ConnectTimeout(self, requests_mock, caplog, client):
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
            MockURLs.environment_availability_url,
            exc=requests.exceptions.ConnectTimeout
        )

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.getEnvironmentAvailability()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text


    def test_getHierarchies_success(self, requests_mock, client):
        requests_mock.request(
            "GET",
            MockURLs.hierarchies_url,
            json=MockResponses.mock_hierarchies
        )
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

        resp = client.getHierarchies()

        assert len(resp["hierarchies"]) == 1
        assert isinstance(resp["hierarchies"], list)
        assert isinstance(resp["hierarchies"][0], dict)
        assert resp["hierarchies"][0]["id"] == "6e292e54-9a26-4be1-9034-607d71492707"


    def test_getHierarchies_raises_HTTPError(self, requests_mock, caplog, client):
        requests_mock.request(
            "GET",
            MockURLs.hierarchies_url,
            exc=requests.exceptions.HTTPError
        )
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

        with pytest.raises(requests.exceptions.HTTPError):
            client.getHierarchies()

        assert "TSIClient: The request to the TSI api returned an unsuccessfull status code." in caplog.text


    def test_getHierarchies_raises_ConnectTimeout(self, requests_mock, caplog, client):
        requests_mock.request(
            "GET",
            MockURLs.hierarchies_url,
            exc=requests.exceptions.ConnectTimeout
        )
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

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.getHierarchies()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text


    def test_getTypes_success(self, requests_mock, client):
        requests_mock.request(
            "GET",
            MockURLs.types_url,
            json=MockResponses.mock_types
        )
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

        resp = client.getTypes()

        assert len(resp["types"]) == 1
        assert isinstance(resp["types"], list)
        assert isinstance(resp["types"][0], dict)
        assert resp["types"][0]["id"] == "1be09af9-f089-4d6b-9f0b-48018b5f7393"


    def test_getTypes_raises_HTTPError(self, requests_mock, caplog, client):
        requests_mock.request(
            "GET",
            MockURLs.types_url,
            exc=requests.exceptions.HTTPError
        )
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

        with pytest.raises(requests.exceptions.HTTPError):
            client.getTypes()

        assert "TSIClient: The request to the TSI api returned an unsuccessfull status code." in caplog.text


    def test_getTypes_raises_ConnectTimeout(self, requests_mock, caplog, client):
        requests_mock.request(
            "GET",
            MockURLs.types_url,
            exc=requests.exceptions.ConnectTimeout
        )
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

        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.getTypes()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text


    def test_getInstances_success(self, requests_mock, client):
        requests_mock.request(
            "GET",
            MockURLs.instances_url,
            json=MockResponses.mock_instances
        )
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

        resp = client.getInstances()

        assert len(resp["instances"]) == 1
        assert isinstance(resp["instances"], list)
        assert isinstance(resp["instances"][0], dict)
        assert resp["instances"][0]["timeSeriesId"][0] == "006dfc2d-0324-4937-998c-d16f3b4f1952"
        assert resp["continuationToken"] == "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="


    def test_getNameById_with_one_correct_id_returns_correct_name(self, requests_mock, client):
        requests_mock.request(
            "GET",
            MockURLs.instances_url,
            json=MockResponses.mock_instances
        )
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

        timeSeriesNames = client.getNameById(
            ids=["006dfc2d-0324-4937-998c-d16f3b4f1952", "made_up_id"]
        )

        assert len(timeSeriesNames) == 2
        assert timeSeriesNames[0] == "F1W7.GS1"
        assert timeSeriesNames[1] == None


    def test_getIdByAssets_with_one_existing_asset_returns_correct_id(self, requests_mock, client):
        requests_mock.request(
            "GET",
            MockURLs.instances_url,
            json=MockResponses.mock_instances
        )
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

        timeSeriesIds = client.getIdByAssets(asset="F1W7")

        assert len(timeSeriesIds) == 1
        assert timeSeriesIds[0] == "006dfc2d-0324-4937-998c-d16f3b4f1952"


    def test_getIdByAssets_with_non_existant_assets_returns_empty_list(self, requests_mock, client):
        requests_mock.request(
            "GET",
            MockURLs.instances_url,
            json=MockResponses.mock_instances
        )
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

        timeSeriesIds = client.getIdByAssets(asset="made_up_asset_name")

        assert len(timeSeriesIds) == 0


    def test_getIdByName_with_one_correct_name_returns_correct_id(self, requests_mock, client):
        requests_mock.request(
            "GET",
            MockURLs.instances_url,
            json=MockResponses.mock_instances
        )
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

        timeSeriesIds = client.getIdByName(
            names=["F1W7.GS1", "made_up_name"]
        )

        assert len(timeSeriesIds) == 2
        assert timeSeriesIds[0] == "006dfc2d-0324-4937-998c-d16f3b4f1952"
        assert timeSeriesIds[1] == None


    def test_getIdByDescription_with_one_correct_description_returns_correct_id(self, requests_mock, client):
        requests_mock.request(
            "GET",
            MockURLs.instances_url,
            json=MockResponses.mock_instances
        )
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

        timeSeriesIds = client.getIdByDescription(
            names=["ContosoFarm1W7_GenSpeed1", "made_up_description"]
        )

        assert len(timeSeriesIds) == 2
        assert timeSeriesIds[0] == "006dfc2d-0324-4937-998c-d16f3b4f1952"
        assert timeSeriesIds[1] == None


    def test_getDataById_returns_data_as_dataframe(self, requests_mock, client):
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
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_success
        )

        data_by_id = client.getDataById(
            timeseries=["006dfc2d-0324-4937-998c-d16f3b4f1952"],
            timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
            interval="PT1S",
            aggregate="avg",
            useWarmStore=False
        )

        assert isinstance(data_by_id, pd.DataFrame)
        assert "timestamp" in data_by_id.columns
        assert "006dfc2d-0324-4937-998c-d16f3b4f1952" in data_by_id.columns
        assert 11 == data_by_id.shape[0]
        assert 2 == data_by_id.shape[1]
        assert data_by_id.at[5, "timestamp"] == "2016-08-01T00:00:15Z"
        assert data_by_id.at[5, "006dfc2d-0324-4937-998c-d16f3b4f1952"] == 66.375


    def test_getDataById_raises_TSIStoreError(self, requests_mock, client):
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
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsistoreerror
        )

        with pytest.raises(TSIStoreError):
            data_by_id = client.getDataById(
                timeseries=["006dfc2d-0324-4937-998c-d16f3b4f1952"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregate="avg",
                useWarmStore=True
            )


    def test_getDataById_raises_TSIQueryError(self, requests_mock, client):
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
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsiqueryerror
        )

        with pytest.raises(TSIQueryError):
            data_by_id = client.getDataById(
                timeseries=["006dfc2d-0324-4937-998c-d16f3b4f1952"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregate="avg",
                useWarmStore=False
            )


    def test_getDataByDescription_returns_data_as_dataframe(self, requests_mock, client):
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
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_success
        )

        data_by_description = client.getDataByDescription(
            variables=["ContosoFarm1W7_GenSpeed1", "DescriptionOfNonExistantTimeseries"],
            TSName=["MyTimeSeriesName", "NameOfNonExistantTimeSeries"],
            timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
            interval="PT1S",
            aggregate="avg",
            useWarmStore=False
        )

        assert isinstance(data_by_description, pd.DataFrame)
        assert "timestamp" in data_by_description.columns
        assert "MyTimeSeriesName" in data_by_description.columns
        assert "NameOfNonExistantTimeSeries" not in data_by_description.columns
        assert 11 == data_by_description.shape[0]
        assert 2 == data_by_description.shape[1]
        assert data_by_description.at[5, "timestamp"] == "2016-08-01T00:00:15Z"
        assert data_by_description.at[5, "MyTimeSeriesName"] == 66.375


    def test_getDataByDescription_raises_TSIStoreError(self, requests_mock, client):
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
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsistoreerror
        )

        with pytest.raises(TSIStoreError):
            data_by_description = client.getDataByDescription(
                variables=["ContosoFarm1W7_GenSpeed1", "DescriptionOfNonExistantTimeseries"],
                TSName=["MyTimeSeriesName", "NameOfNonExistantTimeSeries"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregate="avg",
                useWarmStore=True
            )


    def test_getDataByDescription_raises_TSIQueryError(self, requests_mock, client):
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
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsiqueryerror
        )

        with pytest.raises(TSIQueryError):
            data_by_description = client.getDataByDescription(
                variables=["ContosoFarm1W7_GenSpeed1", "DescriptionOfNonExistantTimeseries"],
                TSName=["MyTimeSeriesName", "NameOfNonExistantTimeSeries"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregate="avg",
                useWarmStore=False
            )


    def test_getDataByName_returns_data_as_dataframe(self, requests_mock, client):
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
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_success
        )

        data_by_name = client.getDataByName(
            variables=["F1W7.GS1", "NameOfNonExistantTimeseries"],
            timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
            interval="PT1S",
            aggregate="avg",
            useWarmStore=False
        )

        assert isinstance(data_by_name, pd.DataFrame)
        assert "timestamp" in data_by_name.columns
        assert "F1W7.GS1" in data_by_name.columns
        assert "NameOfNonExistantTimeSeries" not in data_by_name.columns
        assert 11 == data_by_name.shape[0]
        assert 2 == data_by_name.shape[1]
        assert data_by_name.at[5, "timestamp"] == "2016-08-01T00:00:15Z"
        assert data_by_name.at[5, "F1W7.GS1"] == 66.375


    def test_getDataByName_raises_TSIStoreError(self, requests_mock, client):
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
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsistoreerror
        )

        with pytest.raises(TSIStoreError):
            data_by_name = client.getDataByName(
                variables=["F1W7.GS1", "NameOfNonExistantTimeseries"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregate="avg",
                useWarmStore=True
            )


    def test_getDataByName_raises_TSIQueryError(self, requests_mock, client):
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
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsiqueryerror
        )

        with pytest.raises(TSIQueryError):
            data_by_name = client.getDataByName(
                variables=["F1W7.GS1", "NameOfNonExistantTimeseries"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregate="avg",
                useWarmStore=False
            )
