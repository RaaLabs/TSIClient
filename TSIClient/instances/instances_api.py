from ..authorization.authorization_api import AuthorizationApi
from ..common.common_funcs import CommonFuncs
import json
import requests


class InstancesApi():
    def __init__(
        self,
        application_name: str,
        environment_id: str,
        authorization_api: AuthorizationApi,
        common_funcs: CommonFuncs,
    ):
        self._applicationName = application_name
        self.environmentId = environment_id
        self.authorization_api = authorization_api
        self.common_funcs = common_funcs


    def getInstances(self):
        """Gets all instances (timeseries) from the specified TSI environment.

        Returns:
            dict: The instances in form of the response from the TSI api call.
            Contains typeId, timeSeriesId, name, description, hierarchyIds and instanceFields per instance.

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> instances = client.instances.getInstances()
        """

        authorizationToken = self.authorization_api._getToken()

        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/instances/"
        
        querystring = self.common_funcs._getQueryString()
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

    def writeInstance(self, payload):
        authorizationToken = self.authorization_api._getToken()
        jsonResponse = self.common_funcs._updateTimeSeries(payload, 'instances', self._applicationName, self.environmentId, authorizationToken)
        return jsonResponse


    def deleteInstancesById(self, instances):
        """ instances are the list of the timeseries ids to delete"""
        authorizationToken = self.authorization_api._getToken()
        instancesList = list()
        for i in range(0,len(instances)):
            instance = instances[i]
            if instance == None or len(instance)<36:
                continue
            instancesList.append([instance])
        payload = {"delete":{"timeSeriesIds":instancesList}}
        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/instances/$batch"
        
        querystring = self.common_funcs._getQueryString()
        
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

        return jsonResponse
    
    def deleteInstancesByName(self, instances):
        """ instances are the list of the names to delete"""
        authorizationToken = self.authorization_api._getToken()
        instancesList = list()
        for i in range(0,len(instances)):
            instance = instances[i]
            if instance == None or len(instance)<36:
                continue
            instancesList.append([instance])
        payload = {"delete":{"names":instancesList}}
        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/instances/$batch"
        
        querystring = self.common_funcs._getQueryString()
        
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

        return jsonResponse

    def deleteAllInstances(self):
        instances = self.getInstances()['instances']
        instancesList = list()
        for i in range(0,len(instances)):
            instance = instances[i]['timeSeriesId'][0]
            if instance == None or len(instance)<36:
                continue
            instancesList.append([instance])
        
        authorizationToken = self.authorization_api._getToken()
        payload = {"delete":{"timeSeriesIds":instancesList}}
        
        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/instances/$batch"

        querystring = self.common_funcs._getQueryString()
        
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

        return jsonResponse
