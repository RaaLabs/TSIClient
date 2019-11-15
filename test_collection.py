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
from TSIClient import TSIClient as tsi


class MockURLs():
    """This class holds mock urls that can be used to mock requests to the TSI environment.
    Note that there are dependencies between the MockURLs, the MockResponses and the parameters used
    in "create_TSICLient".
    """

    oauth_url = "https://login.microsoftonline.com/{}/oauth2/token".format("yet_another_tenant_id")
    env_url = "https://api.timeseries.azure.com/environments"
    hierarchies_url = "https://{}.env.timeseries.azure.com/timeseries/hierarchies".format("00000000-0000-0000-0000-000000000000")


class MockRespnses():
    """This class holds mocked request responses which can be used across tests.
    """

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


class TestTSIClient():
    def test_create_TSICLient_sucess(self):
        client = create_TSIClient()
        assert client

    
    def test_getHierarchies(self, requests_mock):
        requests_mock.request(
            "GET",
            MockURLs.hierarchies_url,
            json=MockRespnses.mock_hierarchies
        )
        requests_mock.request(
            "POST",
            MockURLs.oauth_url,
            json=MockRespnses.mock_oauth
        )
        requests_mock.request(
            "GET", 
            MockURLs.env_url, 
            json=MockRespnses.mock_environments
        )

        client = create_TSIClient()
        resp = client.getHierarchies()

        assert len(resp["hierarchies"]) == 1
        assert resp["hierarchies"][0]["id"] == "6e292e54-9a26-4be1-9034-607d71492707"
        

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
