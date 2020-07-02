#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
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

    It can be instantiated either by arguments or by environment variables (if arguments
    are specified, they take precedence even when environment variables are set).

    Args:
        enviroment (str): The name of the Azure TSI environment.
        client_id (str): The client id of the service principal used to authenticate with Azure TSI.
        client_secret (str): The client secret of the service principal used to authenticate with Azure TSI.
        tenant_id (str): The tenant id of the service principal used to authenticate with Azure TSI.
        applicationName (str): The name can be an arbitrary string. For informational purpose.

    Examples:
        The TSIClient is the entry point to the SDK. You can instantiate it like this:

            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient(
            ...     enviroment="<your-tsi-env-name>",
            ...     client_id="<your-client-id>",
            ...     client_secret="<your-client-secret>",
            ...     tenant_id="<your-tenant-id>",
            ...     applicationName="<your-app-name>">
            ... )

        You might find it useful to specify environment variables to instantiate the TSIClient.
        To do so, you need to set the following environment variables:

        * ``TSICLIENT_APPLICATION_NAME``
        * ``TSICLIENT_ENVIRONMENT_NAME``
        * ``TSICLIENT_CLIENT_ID``
        * ``TSICLIENT_CLIENT_SECRET``
        * ``TSICLIENT_TENANT_ID``
        
        Now you can instantiate the TSIClient without passing any arguments:

            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
    """

    def __init__(
            self,
            enviroment=None,
            client_id=None,
            client_secret=None,
            applicationName=None,
            tenant_id=None
        ):
        self._apiVersion = "2018-11-01-preview"
        self._applicationName = applicationName if applicationName is not None else os.environ["TSICLIENT_APPLICATION_NAME"]
        self._enviromentName = enviroment if enviroment is not None else os.environ["TSICLIENT_ENVIRONMENT_NAME"]
        self._client_id = client_id if client_id is not None else os.environ["TSICLIENT_CLIENT_ID"]
        self._client_secret = client_secret if client_secret is not None else os.environ["TSICLIENT_CLIENT_SECRET"]
        self._tenant_id = tenant_id if tenant_id is not None else os.environ["TSICLIENT_TENANT_ID"]


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


    def _getQueryString(self, useWarmStore=None):
        """Creates the querystring for an api request.
        
        Can be used in all api requests in TSIClient.

        Args:
            useWarmStore (bool): A boolean to indicate the storeType. Defaults to None,
                in which case no storeType param is included in the querystring.

        Returns:
            dict: The querystring with the api-version and optionally the storeType.
        """

        if useWarmStore == None:
            return {"api-version": self._apiVersion}

        else:
            return {
                "api-version": self._apiVersion,
                "storeType": "WarmStore" if useWarmStore == True else "ColdStore"
            }


    def _getVariableAggregate(self, aggregate=None):
        """Creates the variable aggregation type and the request type based thereon.

        The request type is either "aggregateSeries" (if an aggregation is provided),
        or "getSeries" if the aggregate is None.

        Args:
            aggregate (str): The aggregation method ("avg", "min", "max").

        Returns:
            tuple: A tuple with the aggregate (dict) and the requestType (str).
        """

        if aggregate not in ["avg", "min", "max", None]:
            raise TSIQueryError(
                "TSIClient: Aggregation method not supported, must be \"avg\", \"min\" or \"max\"."
            )

        if aggregate != None:
            aggregate = {"tsx": "{0!s}($value)".format(aggregate)}
            requestType = "aggregateSeries"
        else:
            requestType = "getSeries"

        return (aggregate, requestType)


    def getEnviroment(self):
        """Gets the id of the environment specified in the TSIClient class constructor.

        Returns:
            str: The environment id.

        Raises:
            TSIEnvironmentError: Raised if the TSI environment does not exist.

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> env = client.getEnviroment()
        """

        authorizationToken = self._getToken()
        url = "https://api.timeseries.azure.com/environments"
        
        querystring = self._getQueryString()
        
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


    def getEnvironmentAvailability(self):
        """Returns the time range and distribution of event count over the event timestamp.
        Can be used to provide landing experience of navigating to the environment.

        Returns:
            dict: The environment availability. Contains interval size, distribution and range.

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> env_availability = client.getEnvironmentAvailability()
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        url = "https://{environmentId}.env.timeseries.azure.com/availability".format(
            environmentId=environmentId,
        )
        querystring = self._getQueryString()
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


    def getInstances(self, filters=None):
        """Gets all or filtered instances (timeseries) from the specified TSI environment.

        If filters is given, only instances that exactly match the filtering strings are returned.

        Args:
            filters (dict(str:list[str])): A dictionary with instance attributes as keys, and
                list of strings as values. Every filter is evaluated individually. Can be None.

        Returns:
            dict: The instances in form of the response from the TSI api call.
            Contains typeId, timeSeriesId, name, description, hierarchyIds and instanceFields per instance.

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> instances = client.getInstances(
            >>>     filters={"hierarchyIds": ["2d1f3876-b6e9-4f98-9db5-e66b5754e755"]}
            >>> )
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()

        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/instances/"
        
        querystring = self._getQueryString()
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
        
        if filters is not None:
            filtered_instances = []
            for key in filters:
                for instance in result['instances']:
                    if key not in instance:
                        logging.warning(f"{key} is not an instance attribute of timeseries instance {instance['timeSeriesId']}, this instance is not returned.")
                        continue

                    for i, _ in enumerate(filters[key]):
                        if filters[key][i] in instance[key]:
                            if instance in filtered_instances:
                                continue

                            filtered_instances.append(instance)

            result["instances"] = filtered_instances

        return result
    

    def getHierarchies(self):
        """Gets all hierarchies from the specified TSI environment.

        Returns:
            dict: The hierarchies in form of the response from the TSI api call.
            Contains hierarchy id, names and source fields per hierarchy.

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> hierarchies = client.getHierarchies()
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()

        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/hierarchies"
        querystring = self._getQueryString()
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
            
        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> types = client.getTypes()
        """
        
        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()

        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/types"
        querystring = self._getQueryString()
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


    def getTypeTsx(self):
        """Extracts type id and Value (tsx) from types from the specified TSI environment.

        Returns:
            dict: The types collected from the response from the TSI api call.
            Contains id and variable value (tsx) per type.
            Only type instances with the JSON build up 
            type > variables > Value > value > tsx 
            are returned.

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> types = client.getTypeTsx()
        """

        types={}
        jsonResponse = self.getTypes()
        
        for typeElement in jsonResponse['types']:
            try:
                typeElement['variables']['Value']['value']['tsx']
                types[typeElement['id']] = typeElement['variables']['Value']['value']['tsx']
            except:
                logging.error('"Value" for type id {type} cannot be extracted'.format(type = typeElement['id']))
                pass

        return types
    
        
    def _updateTimeSeries(self, payload, timeseries):
        """Writes instances to the TSI environment.

        Args:
            payload (str): A json-serializable payload that is posted to the TSI environment.
                The format of the payload is specified in the Azure TSI documentation.

        Returns:
            dict: The response of the TSI api call.
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()

        url = "https://{environmentId}.env.timeseries.azure.com/timeseries/{timeseries}/$batch".format(environmentId=environmentId,timeseries=timeseries)
        
        querystring = self._getQueryString()

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
    
    def writeInstance(self, payload):
        jsonResponse = self._updateTimeSeries(payload, 'instances')
        return jsonResponse

    def writeTypes(self, payload):
        jsonResponse = self._updateTimeSeries(payload, 'types')
        return jsonResponse

    def writeHierarchies(self, payload):
        jsonResponse = self._updateTimeSeries(payload, 'hierarchies')
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
        
        querystring = self._getQueryString()
        
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
        
        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        payload = {"delete":{"timeSeriesIds":instancesList}}
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/instances/$batch"

        querystring = self._getQueryString()
        
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


    def getTypeByDescription(self, names):
        """Returns the type ids that correspond to the given descriptions.

        Args:
            names (list): The descriptions for which to get type ids.

        Returns:
            list: The type ids, None if timeseries description does not exist in the TSI environment.
        """

        result=self.getInstances()
        typeIds=[]
        nameMap={}
        for instance in result['instances']:
            if 'description' in instance:
                nameMap[instance['description']] = instance
        for name in names:
            if name in nameMap:
                typeIds.append(nameMap[name]['typeId'])
            else:
                typeIds.append(None)
        return typeIds

    def getTypeById(self, ids):
        """Returns the type ids that correspond to the given timeseries ids.

        Args:
            ids (list): The timeseries ids for which to get type ids.

        Returns:
            list: The type ids, None if timeseries ids does not exist in the TSI environment.
        """

        result=self.getInstances()
        typeIds=[]
        idMap={}
        for instance in result['instances']:
            if 'timeSeriesId' in instance:
                idMap[instance['timeSeriesId'][0]] = instance
        for ID in ids:
            if ID in idMap:
                typeIds.append(idMap[ID]['typeId'])
            else:
                typeIds.append(None)
        return typeIds


    def getTypeByName(self, names):
        """Returns the type ids that correspond to the given names.

        Args:
            names (list(str)): The names for which to get ids.

        Returns:
            list: The type ids, None if timeseries name does not exist in the TSI environment.
        """

        result=self.getInstances()
        typeIds=[]
        nameMap={}
        for instance in result['instances']:
            if 'name' in instance:
                nameMap[instance['name']] = instance
        for name in names:
            if name in nameMap:
                typeIds.append(nameMap[name]['typeId'])
            else:
                typeIds.append(None)
        return typeIds


    def getDataByName(self, variables, timespan, interval, aggregate=None, useWarmStore=False):
        """Returns a dataframe with timestamps and values for the time series names given in "variables".

        Can be used to return data for single and multiple timeseries. Names must be exact matches.

        Args:
            variables (list): The variable names. Corresponds to the "name/Time Series Name" field of the time series instances.
            timespan (list): A list of two timestamps. First list element ist the start time, second element is the end time.
                Example: timespan=['2019-12-12T15:35:11.68Z', '2019-12-12T17:02:05.958Z']
            interval (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interval="PT1M", for 1 minute aggregation.
            aggregate (str): Supports "min", "max", "avg". Can be None, in which case the raw events are returned. Defaults to None.
            useWarmStore (bool): If True, the query is executed on the warm storage (free of charge), otherwise on the cold storage. Defaults to False.

        Returns:
            A pandas dataframe with timeseries data. Columns are ordered the same way as the variable names.

        Raises:
            TSIStoreError: Raised if the was tried to execute on the warm store, but the warm store is not enabled.
            TSIQueryError: Raised if there was an error in the query arguments (e.g. wrong formatting).

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> data = client.getDataByName(
            ...     variables=["timeseries_name_1", "timeseries_name_2"],
            ...     timespan=["2020-01-25T10:00:11.68Z", "2020-01-26T13:45:11.68Z"],
            ...     interval="PT5M",
            ...     aggregate="avg",
            ...     useWarmStore=False
            ... )
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = self._getQueryString(useWarmStore=useWarmStore)
        timeseries = self.getIdByName(variables)
        types = self.getTypeByName(variables)
        aggregate, requestType = self._getVariableAggregate(aggregate=aggregate)

        return self._getData(
            timeseries=timeseries,
            types = types,
            url=url,
            querystring=querystring,
            requestType=requestType,
            timespan=timespan,
            interval=interval,
            aggregate=aggregate,
            authorizationToken=authorizationToken,
            otherColNamesThanTimeseriesIds=variables,
        )


    def getDataByDescription(self, variables, TSName, timespan, interval, aggregate=None, useWarmStore=False):
        """Returns a dataframe with timestamp and values for the time series that match the description given in "variables".

        Can be used to return data for single and multiple timeseries. Descriptions must be exact matches.

        Args:
            variables (list): The variable descriptions. Corresponds to the "description" field of the time series instances.
            TSName (list): The column names for the refurned dataframe. Must be in the same order as the variable descriptions.
                These names can be arbitrary and do not need to coincide with the timeseries names in TSI. 
            timespan (list): A list of two timestamps. First list element ist the start time, second element is the end time.
                Example: timespan=['2019-12-12T15:35:11.68Z', '2019-12-12T17:02:05.958Z']
            interval (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interval="PT1M", for 1 minute aggregation. If "aggregate" is None, the raw events are returned.
            aggregate (str): Supports "min", "max", "avg". Can be None, in which case the raw events are returned. Defaults to None.
            useWarmStore (bool): If True, the query is executed on the warm storage (free of charge), otherwise on the cold storage. Defaults to False.

        Returns:
            A pandas dataframe with timeseries data. Columns are ordered the same way as the variable descriptions.

        Raises:
            TSIStoreError: Raised if the was tried to execute on the warm store, but the warm store is not enabled.
            TSIQueryError: Raised if there was an error in the query arguments (e.g. wrong formatting).

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> data = client.getDataByDescription(
            ...     variables=["timeseries_description_1", "timeseries_description_2"],
            ...     TSName=["my_timeseries_name_1", "my_timeseries_name_2"]
            ...     timespan=["2020-01-25T10:00:11.68Z", "2020-01-26T13:45:11.68Z"],
            ...     interval="PT5M",
            ...     aggregate="avg",
            ...     useWarmStore=False
            ... )
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = self._getQueryString(useWarmStore=useWarmStore)
        timeseries = self.getIdByDescription(variables)
        types = self.getTypeByDescription(variables)
        aggregate, requestType = self._getVariableAggregate(aggregate=aggregate)

        return self._getData(
            timeseries=timeseries,
            types=types,
            url=url,
            querystring=querystring,
            requestType=requestType,
            timespan=timespan,
            interval=interval,
            aggregate=aggregate,
            authorizationToken=authorizationToken,
            otherColNamesThanTimeseriesIds=TSName
        )


    def getDataById(self, timeseries, timespan, interval, aggregate=None, useWarmStore=False):
        """Returns a dataframe with timestamp and values for the time series that match the description given in "timeseries".

        Can be used to return data for single and multiple timeseries. Timeseries ids must be an exact matches.

        Args:
            timeseries (list): The timeseries ids. Corresponds to the "timeSeriesId" field of the time series instances.
            timespan (list): A list of two timestamps. First list element ist the start time, second element is the end time.
                Example: timespan=['2019-12-12T15:35:11.68Z', '2019-12-12T17:02:05.958Z']
            interval (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interval="PT1M", for 1 minute aggregation. If "aggregate" is None, the raw events are returned.
            aggregate (str): Supports "min", "max", "avg". Can be None, in which case the raw events are returned. Defaults to None.
            useWarmStore (bool): If True, the query is executed on the warm storage (free of charge), otherwise on the cold storage. Defaults to False.

        Returns:
            A pandas dataframe with timeseries data. Columns are ordered the same way as the timeseries ids.

        Raises:
            TSIStoreError: Raised if the was tried to execute on the warm store, but the warm store is not enabled.
            TSIQueryError: Raised if there was an error in the query arguments (e.g. wrong formatting).

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> data = client.getDataById(
            ...     timeseries=["timeseries_id_1", "timeseries_id_2"],
            ...     timespan=["2020-01-25T10:00:11.68Z", "2020-01-26T13:45:11.68Z"],
            ...     interval="PT5M",
            ...     aggregate="avg",
            ...     useWarmStore=False
            ... )
        """

        environmentId = self.getEnviroment()
        authorizationToken = self._getToken()
        url = "https://" + environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = self._getQueryString(useWarmStore=useWarmStore)
        aggregate, requestType = self._getVariableAggregate(aggregate=aggregate)
        types = self.getTypeById(timeseries)

        return self._getData(
            timeseries=timeseries,
            types=types,
            url=url,
            querystring=querystring,
            requestType=requestType,
            timespan=timespan,
            interval=interval,
            aggregate=aggregate,
            authorizationToken=authorizationToken,
        )


    def _getData(
        self,
        timeseries,
        types,
        url,
        querystring,
        requestType,
        timespan,
        interval,
        aggregate,
        authorizationToken,
        otherColNamesThanTimeseriesIds=None,
    ):
        df = None
        typeList = self.getTypeTsx()

        if otherColNamesThanTimeseriesIds != None:
            colNames = otherColNamesThanTimeseriesIds
        else:
            colNames = timeseries

        for i, _ in enumerate(timeseries):
            if timeseries[i] == None:
                logging.error("No such tag: {tag}".format(tag=colNames[i]))
                continue
            logging.info(f'Timeseries {colNames[i]} has type {typeList[types[i]]}')
            payload = {
                requestType: {
                    "timeSeriesId": [timeseries[i]],
                    "timeSeriesName": None,
                    "searchSpan": {"from": timespan[0], "to": timespan[1]},
                    "filter": None,
                    "interval": interval,
                    "inlineVariables": {
                        "AverageTest": {
                            "kind": "numeric",
                            "value": {"tsx": typeList[types[i]]},
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
            try:
                response = requests.request(
                    "POST",
                    url,
                    data=json.dumps(payload),
                    headers=headers,
                    params=querystring,
                )
                response.raise_for_status()
            except requests.exceptions.ConnectTimeout:
                logging.error("TSIClient: The request to the TSI api timed out.")
                raise
            except requests.exceptions.HTTPError:
                logging.error("TSIClient: The request to the TSI api returned an unsuccessfull status code.")
                raise

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
                if response["timestamps"] == []:
                    logging.critical("No data in search span for tag: {tag}".format(tag=colNames[i]))
                    continue

            try:
                assert i == 0
                df = pd.DataFrame(
                    {
                        "timestamp": response["timestamps"],
                        colNames[i]: response["properties"][0]["values"],
                    }
                )
            except:
                df[colNames[i]] = response["properties"][0]["values"]
            finally:
                logging.critical("Loaded data for tag: {tag}".format(tag=colNames[i]))
        return df
