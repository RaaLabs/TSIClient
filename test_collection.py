 # -*- coding: utf-8 -*-
"""
Version: 0.1

RAA-LABS
The digital accelerator for the maritime industry

http://raalabs.com

Project: One operation (ONEOPS)

Purpose: test function for TSI Client package

Description: Makes test for unit testing the functions for TSI Client package

Tests: 
    test_function_works (function) : Function that calls on the TSIClient and
        implements a class instance and asserts that the instance is created.

Output: ok or test failed

TODO: 
    -Create a mock class and test fucntions that asserts that the function 
        calls on the API  and get a response. 
@author: Emil.Ramsvik
@ Email: Emil.Ramsvik@wilhelmsen.com
"""
import pytest
import requests
import requests_mock
from collections import namedtuple
from TSIClient import TSIClient as tsi
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


class TestTSIClient():
    def test_create_TSICLient_success(self):
        client = create_TSIClient()
        assert client


    def test__getToken_success(self, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            json=MockResponses.mock_oauth
        )
        
        client = create_TSIClient()
        token = client._getToken()

        assert token == "some_type token"


    def test__getToken_raises_401_HTTPError(self, requests_mock, caplog):
        httperror_response = namedtuple("httperror_response", "status_code")
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            exc=requests.exceptions.HTTPError(response=httperror_response(status_code=401))
        )
        
        client = create_TSIClient()
        with pytest.raises(requests.exceptions.HTTPError):
            token = client._getToken()

        assert "TSIClient: Authentication with the TSI api was unsuccessful. Check your client secret." in caplog.text


    def test__getToken_raises_ConnectTimeout(self, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            exc=requests.exceptions.ConnectTimeout
        )
        
        client = create_TSIClient()
        with pytest.raises(requests.exceptions.ConnectTimeout):
            token = client._getToken()


    def test_getEnvironment_success(self, requests_mock):
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

        client = create_TSIClient()
        env_id = client.getEnviroment()

        assert env_id == "00000000-0000-0000-0000-000000000000"


    def test_getEnvironment_raises_HTTPError(self, requests_mock, caplog):
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

        client = create_TSIClient()
        with pytest.raises(requests.exceptions.HTTPError):
            env_id = client.getEnviroment()

        assert "TSIClient: The request to the TSI api returned an unsuccessfull status code." in caplog.text


    def test_getEnvironment_raises_ConnectTimeout(self, requests_mock, caplog):
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

        client = create_TSIClient()
        with pytest.raises(requests.exceptions.ConnectTimeout):
            env_id = client.getEnviroment()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text


    def test_getEnvironments_raises_TSIEnvironmentError(self, requests_mock):
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

        client = create_TSIClient()
        with pytest.raises(TSIEnvironmentError) as exc_info:
            client.getEnviroment()

        assert "Azure TSI environment not found. Check the spelling or create an environment in Azure TSI." in str(exc_info.value)

    
    def test_getHierarchies_success(self, requests_mock):
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

        client = create_TSIClient()
        resp = client.getHierarchies()

        assert len(resp["hierarchies"]) == 1
        assert type(resp["hierarchies"]) is list
        assert type(resp["hierarchies"][0]) is dict
        assert resp["hierarchies"][0]["id"] == "6e292e54-9a26-4be1-9034-607d71492707"


    def test_getHierarchies_raises_HTTPError(self, requests_mock, caplog):
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

        client = create_TSIClient()
        with pytest.raises(requests.exceptions.HTTPError):
            resp = client.getHierarchies()

        assert "TSIClient: The request to the TSI api returned an unsuccessfull status code." in caplog.text


    def test_getHierarchies_raises_ConnectTimeout(self, requests_mock, caplog):
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

        client = create_TSIClient()
        with pytest.raises(requests.exceptions.ConnectTimeout):
            resp = client.getHierarchies()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text


    def test_getTypes_success(self, requests_mock):
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

        client = create_TSIClient()
        resp = client.getTypes()

        assert len(resp["types"]) == 1
        assert type(resp["types"]) is list
        assert type(resp["types"][0]) is dict
        assert resp["types"][0]["id"] == "1be09af9-f089-4d6b-9f0b-48018b5f7393"


    def test_getTypes_raises_HTTPError(self, requests_mock, caplog):
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

        client = create_TSIClient()
        with pytest.raises(requests.exceptions.HTTPError):
            resp = client.getTypes()

        assert "TSIClient: The request to the TSI api returned an unsuccessfull status code." in caplog.text


    def test_getTypes_raises_ConnectTimeout(self, requests_mock, caplog):
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

        client = create_TSIClient()
        with pytest.raises(requests.exceptions.ConnectTimeout):
            resp = client.getTypes()

        assert "TSIClient: The request to the TSI api timed out." in caplog.text


    def test_getInstances_success(self, requests_mock):
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

        client = create_TSIClient()
        resp = client.getInstances()

        assert len(resp["instances"]) == 1
        assert type(resp["instances"]) is list
        assert type(resp["instances"][0]) is dict
        assert resp["instances"][0]["timeSeriesId"][0] == "006dfc2d-0324-4937-998c-d16f3b4f1952"
        assert resp["continuationToken"] == "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="


def create_TSIClient():
    """        
    Version: 0.3
    Created on Wed Oct  2 15:54:01 2019
    - Function that creates and instant of the TSIClient class using dummmy input.
    
    Output:
        tsi_keys (class instance) : A class instance with dummy variables
    @author: Siri.Ovregard
    Amended by Emil Ramsvik for use in unit test for TSIClient
    """
    tsi_keys = tsi.TSIClient(
        enviroment='Test_Environment',
        client_id="MyClientID",
        client_secret="a_very_secret_password",
        applicationName="postmanServicePrincipal",
        tenant_id="yet_another_tenant_id"
    )

    return tsi_keys
