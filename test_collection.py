 # -*- coding: utf-8 -*-
"""
Version: 0.1

RAA-LABS
The digital accelerator for the maritime industry

http://raalabs.com

Project: One operation (ONEOPS)

Purpose: test function for TSI Client package

Description: Makes test for unit testing the functions for TSI Client
    
Parameters: 

Output: ok or test failed

@author: Emil.Ramsvik
@ Email: Emil.Ramsvik@wilhelmsen.com
"""
import pytest
from TSIClient import TSIClient as tsi



class TestClass():
    name = 'FakeName'
    def test_function_works(self):
        """Check that the client class can initate """
        client = createTSIClient()
        assert client  

        
"""        
Created on Wed Oct  2 15:54:01 2019

- Functions used in testing environment to initate functions in 
    TSIClient. 


@author: Siri.Ovregard
"""



def createTSIClient():
    tsiKeys = tsi.TSIClient(enviroment = 'Test Environment',
                         client_id = "MyClientID",
                         client_secret = "averysecretpassword",
                         applicationName = "postmanServicePrincipal",
                         tenant_id="yeatanothertenantid")
    return tsiKeys