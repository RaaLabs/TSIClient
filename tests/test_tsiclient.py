import pytest
import requests
from collections import namedtuple
from TSIClient.exceptions import TSIEnvironmentError


class MockURLs():
    """This class holds mock urls that can be used to mock requests to the TSI environment.
    Note that there are dependencies between the MockURLs, the MockResponses and the parameters used
    in "create_TSICLient".
    """

    oauth_url = "https://login.microsoftonline.com/{}/oauth2/token".format("yet_another_tenant_id")
    env_url = "https://api.timeseries.azure.com/environments"
    hierarchies_url = "https://{}.env.timeseries.azure.com/timeseries/hierarchies".format("00000000-0000-0000-0000-000000000000")
    types_url = "https://{}.env.timeseries.azure.com/timeseries/types".format("00000000-0000-0000-0000-000000000000")
    instances_url = "https://{}.env.timeseries.azure.com/timeseries/instances/".format("00000000-0000-0000-0000-000000000000")
    environment_availability_url = "https://{}.env.timeseries.azure.com/availability".format("00000000-0000-0000-0000-000000000000")


class MockResponses():
    """This class holds mocked request responses which can be used across tests.
    The json responses are taken from the official Azure TSI api documentation.
    """
    mock_types = {
        "types": [
            {
                "id": "1be09af9-f089-4d6b-9f0b-48018b5f7393",
                "name": "DefaultType",
                "description": "My Default type",
                "variables": {
                    "EventCount": {
                    "kind": "aggregate",
                    "filter": None,
                    "aggregation": {
                        "tsx": "count()"
                    }
                    }
                }
            }
        ],
        "continuationToken": "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="
    }

    mock_hierarchies = {
        "hierarchies": [
            {
                "id": "6e292e54-9a26-4be1-9034-607d71492707",
                "name": "Location",
                "source": {
                    "instanceFieldNames": [
                    "state",
                    "city"
                    ]
                }
            }
        ],
        "continuationToken": "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="
    }

    mock_oauth = {
        "token_type": "some_type",
        "access_token": "token"
    }

    mock_environments = {
        "environments": [
            {
                "displayName":"Test_Environment",
                "environmentFqdn": "00000000-0000-0000-0000-000000000000.env.timeseries.azure.com",
                "environmentId": "00000000-0000-0000-0000-000000000000",
                "resourceId": "resourceId"
            }
        ]
    }

    mock_instances = {
        "instances": [
            {
                "typeId": "9b84e946-7b36-4aa0-9d26-71bf48cb2aff",
                "name": "F1W7.GS1",
                "timeSeriesId": [
                    "006dfc2d-0324-4937-998c-d16f3b4f1952",
                    "T1"
                ],
                "description": "ContosoFarm1W7_GenSpeed1",
                "hierarchyIds": [
                    "33d72529-dd73-4c31-93d8-ae4e6cb5605d"
                ],
                "instanceFields": {
                    "Name": "GeneratorSpeed",
                    "Plant": "Contoso Plant 1",
                    "Unit": "W7",
                    "System": "Generator System"
                }
            }
        ],
        "continuationToken": "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="
    }

    mock_environment_availability = {
        "availability": {
            "intervalSize": "PT1H",
            "distribution": {
                "2019-03-27T04:00:00Z": 432447,
                "2019-03-27T05:00:00Z": 432340,
                "2019-03-27T06:00:00Z": 432451,
                "2019-03-27T07:00:00Z": 432436,
                "2019-03-26T13:00:00Z": 386247,
                "2019-03-27T00:00:00Z": 436968,
                "2019-03-27T01:00:00Z": 432509,
                "2019-03-27T02:00:00Z": 432487
            },
            "range": {
                "from": "2019-03-14T06:38:27.153Z",
                "to": "2019-03-27T03:57:11.697Z"
            }
        }
    }


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
