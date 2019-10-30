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
from TSIClient import TSIClient as tsi

class TestClass():

    def test_function_works(self):
        """Check that the client class can initate """
        client = create_TSIClient()
        assert client  
        

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
    tsi_keys = tsi.TSIClient(enviroment='Test Environment',
                         client_id="MyClientID",
                         client_secret="a_very_secret_password",
                         applicationName="postmanServicePrincipal",
                         tenant_id="yet_another_tenant_id")
    return tsi_keys
