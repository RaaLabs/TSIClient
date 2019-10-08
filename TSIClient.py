#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 16:23:06 2019
@author: Anders & Sigbjorn
"""
import requests
import json
import pandas as pd

class TSIClient():
    def __init__(self,enviroment,client_id,client_secret,applicationName,tenant_id):
        self._apiVersion = "2018-11-01-preview"
        self._applicationName = applicationName
        self._enviromentName = enviroment
        self._client_id = client_id
        self._client_secret=client_secret
        self._tenant_id = tenant_id
        
    def _getToken(self):
        url = "https://login.microsoftonline.com/{0!s}/oauth2/token".format(self._tenant_id)
        
        payload = {
                "grant_type":"client_credentials",
                 "client_id":self._client_id,
                 "client_secret": self._client_secret,
                 "resource":"https%3A%2F%2Fapi.timeseries.azure.com%2F&undefined="
                   }
        
        payload = "grant_type={grant_type}&client_id={client_id}&client_secret={client_secret}&resource={resource}".format(**payload)


        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'cache-control': "no-cache"
            }
        
        response = requests.request("POST", url, data=payload, headers=headers)
        if response.text:
            jsonResp = json.loads(response.text)
        tokenType = jsonResp['token_type']
        authorizationToken = tokenType +" " + jsonResp['access_token']
        return authorizationToken
        
    def getEnviroment(self):
        
        authorizationToken = self._getToken()
        url = "https://api.timeseries.azure.com/environments"
        
        querystring = {"api-version":self._apiVersion}
        
        payload = ""
        headers = {
            'x-ms-client-application-name': self._applicationName,
            'Authorization': authorizationToken,
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        
        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
        if response.text:
            jsonResponse = json.loads(response.text)
        
        environments = jsonResponse['environments']
        for enviroment in environments:
            if enviroment['displayName']== self._enviromentName:
                environmentId = enviroment['environmentId']
                break
        return environmentId
    
    def getInstances(self):
        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()

        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/instances/"
        
        querystring = {"api-version":self._apiVersion}
        payload = ""
        
        headers = {
            'x-ms-client-application-name': self._applicationName,
            'Authorization': authorizationToken,
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        
        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
        if response.text:
            jsonResponse = json.loads(response.text)
        
        result = jsonResponse
        
        while len(jsonResponse['instances'])>999 and 'continuationToken' in list(jsonResponse.keys()):
            headers = {
                'x-ms-client-application-name': self._applicationName,
                'Authorization': authorizationToken,
                'x-ms-continuation' : jsonResponse['continuationToken'],
                'Content-Type': "application/json",
                'cache-control': "no-cache"
            }
            response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
            if response.text:
                jsonResponse = json.loads(response.text)
            
            result['instances'].extend(jsonResponse['instances'])
            
        return result
        
        
    def writeInstance(self,payload):
        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()

        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/instances/$batch"
        
        print(url)
        querystring = {"api-version":self._apiVersion}
        
        headers = {
            'x-ms-client-application-name': self._applicationName,
            'Authorization': authorizationToken,
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        
        response = requests.request("POST", url, data=json.dumps(payload), headers=headers, params=querystring)
        
        if response.text:
            jsonResponse = json.loads(response.text)
        
        return jsonResponse
    
    def deleteInstances(self,instances):
        
        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        instancesList = list() 
        for i in range(0,len(instances)):
            instance = instances[i]
            if instance == None or len(instance)<36:
                continue
            instancesList.append([instance])
        payload = {"delete":{"timeSeriesIds":instancesList}}
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/instances/$batch"
        
        print(url)
        querystring = {"api-version":self._apiVersion}
        
        headers = {
            'x-ms-client-application-name': self._applicationName,
            'Authorization': authorizationToken,
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        
        response = requests.request("POST", url, data=json.dumps(payload), headers=headers, params=querystring)
        
        # Test if response body contains sth.
        if response.text:
            jsonResponse = json.loads(response.text)
        
        print(jsonResponse)
        
        return jsonResponse
    
    def deleteAllInstances(self):
        instances = self.getInstances()['instances']
        instancesList = list() 
        for i in range(0,len(instances)):
            instance = instances[i]['timeSeriesId'][0]
            if instance == None or len(instance)<36:
                continue
            instancesList.append([instance])
        
        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        payload = {"delete":{"timeSeriesIds":instancesList}}
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/instances/$batch"
        
        print(url)
        querystring = {"api-version":self._apiVersion}
        
        headers = {
            'x-ms-client-application-name': self._applicationName,
            'Authorization': authorizationToken,
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        
        response = requests.request("POST", url, data=json.dumps(payload), headers=headers, params=querystring)
        
        # Test if response body contains sth.
        if response.text:
            jsonResponse = json.loads(response.text)
        
        print(jsonResponse)
        
        return jsonResponse

    def getNameById(self,ids):
        result=self.getInstances()
        timeSeriesNames=[]
        idMap={}
        for instance in result['instances']:
            if 'timeSeriesId' in instance:
                idMap[instance['timeSeriesId'][0]] = instance
        for ID in ids:
            if ID in idMap:
                timeSeriesNames.append(idMap[ID]['name'])
            else:
                timeSeriesNames.append(None)
        return timeSeriesNames    
    
    def getIdByVessels(self,vessel):
        result=self.getInstances()
        timeSeriesIds=[]
        nameMap={}
        for instance in result['instances']:
            if 'name' in instance and vessel in instance['name']:
                nameMap[instance['name']] = instance
                timeSeriesIds.append(instance['timeSeriesId'][0])
            else:
                continue#timeSeriesIds.append(None)
        return timeSeriesIds    

    def getIdByName(self,names):
        result=self.getInstances()
        timeSeriesIds=[]
        nameMap={}
        for instance in result['instances']:
            if 'name' in instance:
                nameMap[instance['name']] = instance
        for name in names:
            if name in nameMap:
                timeSeriesIds.append(nameMap[name]['timeSeriesId'][0])
            else:
                timeSeriesIds.append(None)
        return timeSeriesIds    

    def getIdByDescription(self,names):
        result=self.getInstances()
        timeSeriesIds=[]
        nameMap={}
        for instance in result['instances']:
            if 'description' in instance:
                nameMap[instance['description']] = instance
        for name in names:
            if name in nameMap:
                timeSeriesIds.append(nameMap[name]['timeSeriesId'][0])
            else:
                timeSeriesIds.append(None)
        return timeSeriesIds        
    


    
    def getDataByName(self,variables,timespan,interval,aggregate):
        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        df = None
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = {"api-version":self._apiVersion}
        timeseries = self.getIdByName(variables)
        for i in range(0,len(timeseries)):
            if(timeseries[i] == None):
                print("No such tag: " + variables[i])
                continue
            payload = {
                    "aggregateSeries": {"timeSeriesId": [timeseries[i]],
                                        "timeSeriesName" : None,
                                        "searchSpan": {
                                                "from": timespan[0],
                                                "to": timespan[1]
                                                },
                                         "filter": None,
                                         "interval": interval,
                                         "inlineVariables": {
                                                 "AverageTest": {
                                                         "kind": "numeric",
                                                         "value": {"tsx": "$event.value"},
                                                         "filter": None,
                                                         "aggregation": {"tsx": "{0!s}($value)".format(aggregate)}
                                                         },
    
                                                 },                            
                                        "projectedVariables": ["AverageTest"]
                                        }
                                        }
                                                 
            headers = {
                'x-ms-client-application-name': self._applicationName,
                'Authorization': authorizationToken,
                'Content-Type': "application/json",
                'cache-control': "no-cache"
            }
            response = requests.request("POST", url, data=json.dumps(payload), headers=headers,params=querystring)
            
            # Test if response body contains sth.
            if response.text:
                response = json.loads(response.text)
            # Handle error if deserialization fails (because of no text or bad format)
            try:
                assert i == 0
                df=pd.DataFrame({'timestamp':response['timestamps'],variables[i]:response['properties'][0]['values']})
            except:
                df[variables[i]]=response['properties'][0]['values']                
            finally:
                print("Loaded data for tag: " + variables[i])               
        return df


    def getDataByDescription(self, variables, timespan, interval, aggregate, columnNames):
        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        df = None
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = {"api-version":self._apiVersion}
        timeseries = self.getIdByDescription(variables)
        if aggregate != None:
            aggregate={"tsx": "{0!s}($value)".format(aggregate)}
            dict_key = "aggregateSeries"
        else:
            dict_key = "getSeries"
            
        for i in range(0,len(timeseries)):
            if(timeseries[i] == None):
                print("No such tag: " + variables[i])
                continue
            payload = {
                    dict_key : {"timeSeriesId": [timeseries[i]],
                                        "timeSeriesName" : None,
                                        "searchSpan": {
                                                "from": timespan[0],
                                                "to": timespan[1]
                                                },
                                         "filter": None,
                                         "interval": interval,
                                         "inlineVariables": {
                                                 "AverageTest": {
                                                         "kind": "numeric",
                                                         "value": {"tsx": "$event.value"},
                                                         "filter": None,
                                                         "aggregation": aggregate
                                                         },
    
                                                 },                            
                                        "projectedVariables": ["AverageTest"]
                                        }
                                        }
                                                 
            headers = {
                'x-ms-client-application-name': self._applicationName,
                'Authorization': authorizationToken,
                'Content-Type': "application/json",
                'cache-control': "no-cache"
            }
            response = requests.request("POST", url, data=json.dumps(payload), headers=headers,params=querystring)
            
            # Test if response body contains sth.
            if response.text:
                response = json.loads(response.text)
            # Handle error if deserialization fails (because of no text or bad format)
            try:
                assert i == 0
                df=pd.DataFrame({'timestamp':response['timestamps'],columnNames[i]:response['properties'][0]['values']})
            except:
                df[columnNames[i]]=response['properties'][0]['values']                
            finally:
                print("Loaded data for tag: " + columnNames[i])               
        return df
