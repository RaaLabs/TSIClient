#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import pandas as pd
import requests
import logging
from TSIClient.exceptions import TSIEnvironmentError
from TSIClient.exceptions import TSIStoreError
from TSIClient.exceptions import TSIQueryError


class TSIClient():
    """TSIClient. Holds methods to interact with an Azure TSI environment.

    This class can be used to retrieve time series data from Azure TSI. Data
    is retrieved in form of a pandas dataframe, which allows subsequent analysis
    by data analysts, data scientists and developers.

    Args:
        enviroment (str): The name of the Azure TSI environment.
        client_id (str): The client id of the service principal used to authenticate with Azure TSI.
        client_secret (str): The client secret of the service principal used to authenticate with Azure TSI.
        tenant_id (str): The tenant id of the service principal used to authenticate with Azure TSI.
        applicationName (str): The name can be an arbitrary string. For informational purpose.

    Example:
        The TSIClient is the entry point to the SDK. You can instantiate it like this:

            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient(
            ...     enviroment="<your-tsi-env-name>",
            ...     client_id="<your-client-id>",
            ...     client_secret="<your-client-secret>",
            ...     tenant_id="<your-tenant-id>",
            ...     applicationName="<your-app-name>">
            ... )
    """

    def __init__(self, enviroment, client_id, client_secret, applicationName, tenant_id):
        self._apiVersion = "2018-11-01-preview"
        self._applicationName = applicationName
        self._enviromentName = enviroment
        self._client_id = client_id
        self._client_secret=client_secret
        self._tenant_id = tenant_id


    def _getToken(self):
        """Gets an authorization token from the Azure TSI api which is used to authenticate api calls.

        Returns:
            str: The authorization token.
        """

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

        try:
            response = requests.request("POST", url, data=payload, headers=headers, timeout=10)
            response.raise_for_status()
        except requests.exceptions.ConnectTimeout:
            logging.error("TSIClient: The request to the TSI api timed out.")
            raise
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            if status_code == 401:
                logging.error("TSIClient: Authentication with the TSI api was unsuccessful. Check your client secret.")
            else:
                logging.error("TSIClient: The request to the TSI api returned an unsuccessfull status code. Check the stack trace")
            raise

        jsonResp = json.loads(response.text)
        tokenType = jsonResp['token_type']
        authorizationToken = tokenType +" " + jsonResp['access_token']
        return authorizationToken


    def getEnviroment(self):
        """Gets the id of the environment specified in the TSIClient class constructor.

        Returns:
            str: The environment id.

        Raises:
            TSIEnvironmentError: Raised if the TSI environment does not exist.
        """

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
        
        try:
            response = requests.request("GET", url, data=payload, headers=headers, params=querystring, timeout=10)
            response.raise_for_status()
        except requests.exceptions.ConnectTimeout:
            logging.error("TSIClient: The request to the TSI api timed out.")
            raise
        except requests.exceptions.HTTPError:
            logging.error("TSIClient: The request to the TSI api returned an unsuccessfull status code.")
            raise

        environments = json.loads(response.text)['environments']
        environmentId = None
        for enviroment in environments:
            if enviroment['displayName'] == self._enviromentName:
                environmentId = enviroment['environmentId']
                break
        if environmentId == None:
            raise TSIEnvironmentError("TSIClient: TSI environment not found. Check the spelling or create an environment in Azure TSI.")

        return environmentId
    

    def getInstances(self):
        """Gets all instances (timeseries) from the specified TSI environment.

        Returns:
            dict: The instances in form of the response from the TSI api call.
            Contains typeId, timeSeriesId, name, description, hierarchyIds and instanceFields per instance.
        """

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
    

    def getHierarchies(self):
        """Gets all hierarchies from the specified TSI environment.

        Returns:
            dict: The hierarchies in form of the response from the TSI api call.
            Contains hierarchy id, names and source fields per hierarchy.
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()

        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/hierarchies"
        querystring = {"api-version":self._apiVersion}
        payload = ""
        headers = {
            'x-ms-client-application-name': self._applicationName,
            'Authorization': authorizationToken,
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }

        try:
            response = requests.request(
                "GET",
                url,
                data=payload,
                headers=headers,
                params=querystring,
                timeout=10
            )
            response.raise_for_status()
        except requests.exceptions.ConnectTimeout:
            logging.error("TSIClient: The request to the TSI api timed out.")
            raise
        except requests.exceptions.HTTPError:
            logging.error("TSIClient: The request to the TSI api returned an unsuccessfull status code.")
            raise

        return json.loads(response.text)


    def getTypes(self):
        """Gets all types from the specified TSI environment.

        Returns:
            dict: The types in form of the response from the TSI api call.
            Contains id, name, description and variables per type.
        """
        
        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()

        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/types"
        querystring = {"api-version":self._apiVersion}
        payload = ""
        headers = {
            'x-ms-client-application-name': self._applicationName,
            'Authorization': authorizationToken,
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }

        try:
            response = requests.request(
                "GET",
                url,
                data=payload,
                headers=headers,
                params=querystring,
                timeout=10
            )
            response.raise_for_status()

        except requests.exceptions.ConnectTimeout:
            logging.error("TSIClient: The request to the TSI api timed out.")
            raise
        except requests.exceptions.HTTPError:
            logging.error("TSIClient: The request to the TSI api returned an unsuccessfull status code.")
            raise

        return json.loads(response.text)

        
    def writeInstance(self, payload):
        """Writes instances to the TSI environment.

        Args:
            payload (str): A json-serializable payload that is posted to the TSI environment.
                The format of the payload is specified in the Azure TSI documentation.

        Returns:
            dict: The response of the TSI api call.
        """

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


    def deleteInstances(self, instances):
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


    def getNameById(self, ids):
        """Returns the timeseries names that correspond to the given ids.

        Args:
            ids (list): The ids for which to get names.

        Returns:
            list: The timeseries names, None if timeseries id does not exist in the TSI environment.
        """

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


    def getIdByAssets(self, asset):
        """Returns the timeseries ids that belong to a given asset.

        Args:
            asset (str): The asset name.

        Returns:
            list: The timeseries ids. 
        """

        result=self.getInstances()
        timeSeriesIds=[]
        nameMap={}
        for instance in result['instances']:
            if 'name' in instance and asset in instance['name']:
                nameMap[instance['name']] = instance
                timeSeriesIds.append(instance['timeSeriesId'][0])
            else:
                continue#timeSeriesIds.append(None)
        return timeSeriesIds    


    def getIdByName(self, names):
        """Returns the timeseries ids that correspond to the given names.

        Args:
            names (list(str)): The names for which to get ids.

        Returns:
            list: The timeseries ids, None if timeseries name does not exist in the TSI environment.
        """

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


    def getIdByDescription(self, names):
        """Returns the timeseries ids that correspond to the given descriptions.

        Args:
            names (list): The descriptions for which to get ids.

        Returns:
            list: The timeseries ids, None if timeseries description does not exist in the TSI environment.
        """
        
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
    

    def getDataByName(self, variables, timespan, interval, aggregate, useWarmStore=False):
        """Returns a dataframe with timestamps and values for the time series names given in "variables".

        Can be used to return data for single and multiple timeseries. Names must be exact matches.

        Args:
            variables (list): The variable names. Corresponds to the "name/Time Series Name" field of the time series instances.
            timespan (list): A list of two timestamps. First list element ist the start time, second element is the end time.
                Example: timespan=['2019-12-12T15:35:11.68Z', '2019-12-12T17:02:05.958Z']
            interval (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interval="PT1M", for 1 minute aggregation.
            aggregate (str): Supports "min", "max", "avg". Cannot be None.
            useWarmStore (bool): If True, the query is executed on the warm storage (free of charge), otherwise on the cold storage. Defaults to False.

        Returns:
            A pandas dataframe with timeseries data.

        Raises:
            TSIStoreError: Raised if the was tried to execute on the warm store, but the warm store is not enabled.
            TSIQueryError: Raised if there was an error in the query arguments (e.g. wrong formatting).
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        df = None
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = {
            "api-version": self._apiVersion,
            "storeType": "WarmStore" if useWarmStore == True else "ColdStore"
        }
        timeseries = self.getIdByName(variables)
        for i in range(0, len(timeseries)):
            if timeseries[i] == None:
                logging.error("No such tag: {tag}".format(tag=variables[i]))
                continue
            payload = {
                "aggregateSeries": {
                    "timeSeriesId": [timeseries[i]],
                    "timeSeriesName": None,
                    "searchSpan": {"from": timespan[0], "to": timespan[1]},
                    "filter": None,
                    "interval": interval,
                    "inlineVariables": {
                        "AverageTest": {
                            "kind": "numeric",
                            "value": {"tsx": "$event.value"},
                            "filter": None,
                            "aggregation": {"tsx": "{0!s}($value)".format(aggregate)},
                        },
                    },
                    "projectedVariables": ["AverageTest"],
                }
            }

            headers = {
                "x-ms-client-application-name": self._applicationName,
                "Authorization": authorizationToken,
                "Content-Type": "application/json",
                "cache-control": "no-cache",
            }
            response = requests.request(
                "POST",
                url,
                data=json.dumps(payload),
                headers=headers,
                params=querystring,
            )

            # Test if response body contains sth.
            if response.text:
                response = json.loads(response.text)
                if "error" in response:
                    if "innerError" in response["error"]:
                        if response["error"]["innerError"]["code"] == "TimeSeriesQueryNotSupported":
                            raise TSIStoreError(
                                "TSIClient: Warm store not enabled in TSI environment: {id}. Set useWarmStore to False."
                                    .format(id=self._enviromentName),
                            )
                    else:
                        logging.error("TSIClient: The query was unsuccessful, check the format of the function arguments.")
                        raise TSIQueryError(response["error"])

            try:
                assert i == 0
                df = pd.DataFrame(
                    {
                        "timestamp": response["timestamps"],
                        variables[i]: response["properties"][0]["values"],
                    }
                )
            except:
                df[variables[i]] = response["properties"][0]["values"]
            finally:
                logging.critical("Loaded data for tag: {tag}".format(tag=variables[i]))
        return df


    def getDataByDescription(self, variables, TSName, timespan, interval, aggregate, useWarmStore=False):
        """Returns a dataframe with timestamp and values for the time series that match the description given in "variables".

        Can be used to return data for single and multiple timeseries. Descriptions must be exact matches.

        Args:
            variables (list): The variable descriptions. Corresponds to the "description" field of the time series instances.
            TSName (list): The column names for the refurned dataframe. Must be in the same order as the variable descriptions.
            timespan (list): A list of two timestamps. First list element ist the start time, second element is the end time.
                Example: timespan=['2019-12-12T15:35:11.68Z', '2019-12-12T17:02:05.958Z']
            interval (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interval="PT1M", for 1 minute aggregation. If "aggregate" is None, the raw events are returned.
            aggregate (str): Supports "min", "max", "avg". Can be None, in which case the raw events are returned.
            useWarmStore (bool): If True, the query is executed on the warm storage (free of charge), otherwise on the cold storage. Defaults to False.

        Returns:
            A pandas dataframe with timeseries data.

        Raises:
            TSIStoreError: Raised if the was tried to execute on the warm store, but the warm store is not enabled.
            TSIQueryError: Raised if there was an error in the query arguments (e.g. wrong formatting).
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        df = None
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = {
            "api-version": self._apiVersion,
            "storeType": "WarmStore" if useWarmStore == True else "ColdStore"
        }
        timeseries = self.getIdByDescription(variables)
        if aggregate != None:
            aggregate = {"tsx": "{0!s}($value)".format(aggregate)}
            dict_key = "aggregateSeries"
        else:
            dict_key = "getSeries"

        for i in range(0, len(timeseries)):
            if timeseries[i] == None:
                logging.error("No such tag: {tag}".format(tag=variables[i]))
                continue
            payload = {
                dict_key: {
                    "timeSeriesId": [timeseries[i]],
                    "timeSeriesName": None,
                    "searchSpan": {"from": timespan[0], "to": timespan[1]},
                    "filter": None,
                    "interval": interval,
                    "inlineVariables": {
                        "AverageTest": {
                            "kind": "numeric",
                            "value": {"tsx": "$event.value"},
                            "filter": None,
                            "aggregation": aggregate,
                        },
                    },
                    "projectedVariables": ["AverageTest"],
                }
            }

            headers = {
                "x-ms-client-application-name": self._applicationName,
                "Authorization": authorizationToken,
                "Content-Type": "application/json",
                "cache-control": "no-cache",
            }
            response = requests.request(
                "POST",
                url,
                data=json.dumps(payload),
                headers=headers,
                params=querystring,
            )

            # Test if response body contains sth.
            if response.text:
                response = json.loads(response.text)
                if "error" in response:
                    if "innerError" in response["error"]:
                        if response["error"]["innerError"]["code"] == "TimeSeriesQueryNotSupported":
                            raise TSIStoreError(
                                "TSIClient: Warm store not enabled in TSI environment: {id}. Set useWarmStore to False."
                                    .format(id=self._enviromentName),
                            )
                    else:
                        logging.error("TSIClient: The query was unsuccessful, check the format of the function arguments.")
                        raise TSIQueryError(response["error"])

            try:
                assert i == 0
                df = pd.DataFrame(
                    {
                        "timestamp": response["timestamps"],
                        TSName[i]: response["properties"][0]["values"],
                    }
                )
            except:
                df[TSName[i]] = response["properties"][0]["values"]
            finally:
                logging.critical("Loaded data for tag: {tag}".format(tag=TSName[i]))
        return df


    def getDataById(self, timeseries, timespan, interval, aggregate, useWarmStore=False):
        """Returns a dataframe with timestamp and values for the time series that match the description given in "timeseries".

        Can be used to return data for single and multiple timeseries. Timeseries ids must be an exact matches.

        Args:
            timeseries (list): The timeseries ids. Corresponds to the "timeSeriesId" field of the time series instances.
            timespan (list): A list of two timestamps. First list element ist the start time, second element is the end time.
                Example: timespan=['2019-12-12T15:35:11.68Z', '2019-12-12T17:02:05.958Z']
            interval (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interval="PT1M", for 1 minute aggregation. If "aggregate" is None, the raw events are returned.
            aggregate (str): Supports "min", "max", "avg". Can be None, in which case the raw events are returned.
            useWarmStore (bool): If True, the query is executed on the warm storage (free of charge), otherwise on the cold storage. Defaults to False.

        Returns:
            A pandas dataframe with timeseries data.

        Raises:
            TSIStoreError: Raised if the was tried to execute on the warm store, but the warm store is not enabled.
            TSIQueryError: Raised if there was an error in the query arguments (e.g. wrong formatting).
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        df = None
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = {
            "api-version": self._apiVersion,
            "storeType": "WarmStore" if useWarmStore == True else "ColdStore"
        }
        if aggregate != None:
            aggregate = {"tsx": "{0!s}($value)".format(aggregate)}
            dict_key = "aggregateSeries"
        else:
            dict_key = "getSeries"

        for i in range(0, len(timeseries)):
            if timeseries[i] == None:
                logging.error("No such tag: {tag}".format(tag=timeseries[i]))
                continue
            payload = {
                dict_key: {
                    "timeSeriesId": [timeseries[i]],
                    "timeSeriesName": None,
                    "searchSpan": {"from": timespan[0], "to": timespan[1]},
                    "filter": None,
                    "interval": interval,
                    "inlineVariables": {
                        "AverageTest": {
                            "kind": "numeric",
                            "value": {"tsx": "$event.value"},
                            "filter": None,
                            "aggregation": aggregate,
                        },
                    },
                    "projectedVariables": ["AverageTest"],
                }
            }

            headers = {
                "x-ms-client-application-name": self._applicationName,
                "Authorization": authorizationToken,
                "Content-Type": "application/json",
                "cache-control": "no-cache",
            }
            response = requests.request(
                "POST",
                url,
                data=json.dumps(payload),
                headers=headers,
                params=querystring,
            )

            if response.text:
                response = json.loads(response.text)
                if "error" in response:
                    if "innerError" in response["error"]:
                        if response["error"]["innerError"]["code"] == "TimeSeriesQueryNotSupported":
                            raise TSIStoreError(
                                "TSIClient: Warm store not enabled in TSI environment: {id}. Set useWarmStore to False."
                                    .format(id=self._enviromentName),
                            )
                    else:
                        logging.error("TSIClient: The query was unsuccessful, check the format of the function arguments.")
                        raise TSIQueryError(response["error"])

            try:
                assert i == 0
                df = pd.DataFrame(
                    {
                        "timestamp": response["timestamps"],
                        timeseries[i]: response["properties"][0]["values"],
                    }
                )
            except:
                df[timeseries[i]] = response["properties"][0]["values"]
            finally:
                logging.critical("Loaded data for tag: {tag}".format(tag=timeseries[i]))
        return df
