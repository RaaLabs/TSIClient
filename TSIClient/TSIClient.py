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
        api_version (str): The TSI api version (optional, allowed values: '2018-11-01-preview' and '2020-07-31').
            Defaults to '2020-07-31'.

    Examples:
        The TSIClient is the entry point to the SDK. You can instantiate it like this:

            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient(
            ...     enviroment="<your-tsi-env-name>",
            ...     client_id="<your-client-id>",
            ...     client_secret="<your-client-secret>",
            ...     tenant_id="<your-tenant-id>",
            ...     applicationName="<your-app-name>">,
            ...     api_version="2020-07-31"
            ... )

        You might find it useful to specify environment variables to instantiate the TSIClient.
        To do so, you need to set the following environment variables:

        * ``TSICLIENT_APPLICATION_NAME``
        * ``TSICLIENT_ENVIRONMENT_NAME``
        * ``TSICLIENT_CLIENT_ID``
        * ``TSICLIENT_CLIENT_SECRET``
        * ``TSICLIENT_TENANT_ID``
        * ``TSI_API_VERSION``
        
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
            tenant_id=None,
            api_version=None
        ):
        self._applicationName = applicationName if applicationName is not None else os.environ["TSICLIENT_APPLICATION_NAME"]
        self._enviromentName = enviroment if enviroment is not None else os.environ["TSICLIENT_ENVIRONMENT_NAME"]
        self._client_id = client_id if client_id is not None else os.environ["TSICLIENT_CLIENT_ID"]
        self._client_secret = client_secret if client_secret is not None else os.environ["TSICLIENT_CLIENT_SECRET"]
        self._tenant_id = tenant_id if tenant_id is not None else os.environ["TSICLIENT_TENANT_ID"]

        allowed_api_versions = ["2020-07-31", "2018-11-01-preview"]
        if api_version in allowed_api_versions:
            self._apiVersion = api_version
        elif "TSI_API_VERSION" in os.environ:
            if os.environ["TSI_API_VERSION"] in allowed_api_versions:
                self._apiVersion = os.environ["TSI_API_VERSION"]
        else:
            self._apiVersion = "2020-07-31"


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


    def _getVariableAggregate(self, aggregate=None, interpolationKind=None, interpolationSpan=None):
        """Creates the fields of the payload corresponding to the inlineVariable 
            and to the projectedVariables name

        Args:
            aggregate (str): The aggregation method to be used without interpolation:
                ("min", "max", "sum", "avg", "first", "last", "median", "stdev").
            The aggregation method to be used with interpolation:
                ("twsum", "twavg", "left", "right")
            interpolationKind (str): Type of interpolation technique: ("Linear", "Step")
            interpolationSpan (str): The time range to the left and right of 
            the search span to be used for Interpolation.

        Returns:
            tuple: A tuple with the inlineVar (dict) and the variableName (str).
        """

        if aggregate not in ["min", "max", "sum", "avg", "first", "last", "median","stdev",\
             "twsum", "twavg", "left", "right", None]:
            raise TSIQueryError(
                "TSIClient: Aggregation method not supported, must be \"min\", \"max\"," \
                    "\"sum\", \"avg\", \"first\", \"last\", \"median\", \"stdev\", "\
                    "\"twsum\", \"twavg\", \"left\" or \"right\"."
            )
        inlineVar = None

        if aggregate != None:
            if aggregate in ["twsum", "twavg", "left", "right"]:
                if interpolationKind==None:
                    raise TSIQueryError(
                    "TSIClient: Aggregation method not supported without interpolation."\
                        "Interpolation type must be either \"Linear\" or \"Step\"."
                    )
                if interpolationSpan==None:
                    raise TSIQueryError(
                    "TSIClient: Aggregation method not supported without interpolation."\
                        "Need interpolation boundary."
                    )
                variableName = interpolationKind.capitalize() + aggregate.capitalize() + 'Interpolation'

                inlineVar = {"kind":"numeric", "value": {"tsx": "$event.value"}, "filter": None,\
                    "interpolation":{"kind": "{0!s}".format(interpolationKind),\
                    "boundary":{"span": "{0!s}".format(interpolationSpan)}},\
                    "aggregation": {"tsx": "{0!s}($value)".format(aggregate)}}
                    
                
            else: 
                variableName = aggregate.capitalize() + 'VarAggregate'

                inlineVar = {"kind":"numeric", "value": {"tsx": "$event.value"}, "filter": None,\
                             "aggregation": {"tsx": "{0!s}($value)".format(aggregate)}}
                    
                

        return (inlineVar, variableName)


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


    def getInstances(self):
        """Gets all instances (timeseries) from the specified TSI environment.

        Returns:
            dict: The instances in form of the response from the TSI api call.
            Contains typeId, timeSeriesId, name, description, hierarchyIds and instanceFields per instance.

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> instances = client.getInstances()
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
    
    def getRequestType(self, aggregate=None, requestBodyType=None):
        if aggregate == None and (requestBodyType == None or requestBodyType=="getSeries"):
            requestType = "getSeries"
        elif aggregate != None and (requestBodyType == None or requestBodyType=="aggregateSeries"):
            requestType = "aggregateSeries"
        elif requestBodyType == "getEvents":
            requestType = "getEvents"
        else:
            raise TSIQueryError(
                "TSIClient: request body types are either getSeries, aggregateSeries or getEvents"
            )
        return requestType
    
    def getInlineVariablesAggregate(self, aggregateList=None, interpolationList=None, interpolationSpanList=None):
        """Returns a tuple of lists to apply in the payload consisiting of the InlineVariables and the 
            projectedVariables. 

        Args:
            aggregateList (list): List of the aggregation methods to be used without interpolation:
                ("min", "max", "sum", "avg", "first", "last", "median", "stdev").
            The aggregation method to be used with interpolation:
                ("twsum", "twavg", "left", "right")
            interpolationList (list): A list of interpolation methods. Either Linear or Step.
            interpolationSpanList (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interpolation Boundary span ="P1D", for 1 day to the left and right of the search span to be used for Interpolation..

        Returns:
            A tuple of lists to apply in the payload consisiting of the InlineVariables and the 
            projectedVariables. 

        Raises:
            TSIQueryError: Raised if there was an error in the aggregation lists, 
            the list have either different length or interpolation aggregation 
            is given but there is not given specifications for the interpolation 
            kind and the interpolation boundary.
            If aggregates given do not require interpolation, the interpolation lists are not needed.


        """
        if isinstance(aggregateList, list) and isinstance(interpolationList, list) and isinstance(interpolationSpanList, list):
            if not len(aggregateList)==len(interpolationList)==len(interpolationSpanList):
                raise TSIQueryError(
                    "TSIClient: All Aggregate lists must be of the same length"
                )
            projectedVarNames = []
            inlineVarPayload = []
            for i in range(0,len(aggregateList)):
                (inlineVar, variableName) = self._getVariableAggregate(aggregate=aggregateList[i],\
                    interpolationKind=interpolationList[i], interpolationSpan=interpolationSpanList[i])
                projectedVarNames.append(variableName)
                inlineVarPayload.append(inlineVar)
                
        elif isinstance(aggregateList, str) and (isinstance(interpolationList, str) or interpolationList == None)\
            and (isinstance(interpolationSpanList, str) or interpolationSpanList == None):
                
            projectedVarNames = []
            inlineVarPayload = []
            (inlineVar, variableName) = self._getVariableAggregate(aggregate=aggregateList,\
                interpolationKind=interpolationList, interpolationSpan=interpolationSpanList)
            projectedVarNames.append(variableName)
            inlineVarPayload.append(inlineVar)
            
        else:
            raise TSIQueryError(
                "TSIClient: If aggregate list contains aggregations requiring interpolation, "\
                    "both the aggregate and interpolation specifications must be lists"
            )
            
        return (inlineVarPayload, projectedVarNames)
    
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


    def getDataByName(self, variables, timespan, interval, aggregateList=None, interpolationList=None, interpolationSpanList=None, requestBodyType=None, useWarmStore=False):
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
        
        if isinstance(aggregateList, list):
            aggregate = aggregateList[0]
            if not (isinstance(interpolationList, list) and isinstance(interpolationSpanList, list)):
                aggArray = ["min", "max", "sum", "avg", "first", "last", "median","stdev"]
                """ Check if all aggragates are methods to be used without interpolation """
                nonInterpolation = all(elem in aggArray for elem in aggregateList)
                if nonInterpolation:
                    interpolationList = [None]*len(aggregateList)
                    interpolationSpanList = [None]*len(aggregateList)
                         
        else:
            aggregate = aggregateList
        requestType = self.getRequestType(aggregate=aggregate,requestBodyType=requestBodyType)

        return self._getData(
            timeseries=timeseries,
            types = types,
            url=url,
            querystring=querystring,
            requestType=requestType,
            timespan=timespan,
            interval=interval,
            aggregateList=aggregateList,
            interpolationList=interpolationList,
            interpolationSpanList=interpolationSpanList,
            authorizationToken=authorizationToken,
            otherColNamesThanTimeseriesIds=variables,
        )


    def getDataByDescription(self, variables, TSName, timespan, interval, aggregateList=None, interpolationList=None, interpolationSpanList=None, requestBodyType=None,useWarmStore=False):
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
        
        if isinstance(aggregateList, list):
            aggregate = aggregateList[0]
            if not (isinstance(interpolationList, list) and isinstance(interpolationSpanList, list)):
                aggArray = ["min", "max", "sum", "avg", "first", "last", "median","stdev"]
                """ Check if all aggragates are methods to be used without interpolation """
                nonInterpolation = all(elem in aggArray for elem in aggregateList)
                if nonInterpolation:
                    interpolationList = [None]*len(aggregateList)
                    interpolationSpanList = [None]*len(aggregateList)
                         
        else:
            aggregate = aggregateList
        requestType = self.getRequestType(aggregate=aggregate,requestBodyType=requestBodyType)
        
        return self._getData(
            timeseries=timeseries,
            types=types,
            url=url,
            querystring=querystring,
            requestType=requestType,
            timespan=timespan,
            interval=interval,
            aggregateList=aggregateList,
            interpolationList=interpolationList,
            interpolationSpanList=interpolationSpanList,
            authorizationToken=authorizationToken,
            otherColNamesThanTimeseriesIds=TSName
        )


    def getDataById(self, timeseries, timespan, interval, aggregateList=None, interpolationList=None, interpolationSpanList=None, requestBodyType=None, useWarmStore=False):
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
        types = self.getTypeById(timeseries)
        
        if isinstance(aggregateList, list):
            aggregate = aggregateList[0]
            if not (isinstance(interpolationList, list) and isinstance(interpolationSpanList, list)):
                aggArray = ["min", "max", "sum", "avg", "first", "last", "median","stdev"]
                """ Check if all aggragates are methods to be used without interpolation """
                nonInterpolation = all(elem in aggArray for elem in aggregateList)
                if nonInterpolation:
                    interpolationList = [None]*len(aggregateList)
                    interpolationSpanList = [None]*len(aggregateList)
                         
        else:
            aggregate = aggregateList
        requestType = self.getRequestType(aggregate=aggregate,requestBodyType=requestBodyType)
        
        return self._getData(
            timeseries=timeseries,
            types=types,
            url=url,
            querystring=querystring,
            requestType=requestType,
            timespan=timespan,
            interval=interval,
            aggregateList=aggregateList,
            interpolationList=interpolationList,
            interpolationSpanList=interpolationSpanList,
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
        aggregateList,
        interpolationList,
        interpolationSpanList,
        authorizationToken,
        otherColNamesThanTimeseriesIds=None,
    ):
        df = None
        typeList = self.getTypeTsx()
                
        if otherColNamesThanTimeseriesIds != None:
            colNames = otherColNamesThanTimeseriesIds
        else:
            colNames = timeseries
        
        if requestType == 'aggregateSeries':
            (inlineVarPayload, projectedVarNames) = self.getInlineVariablesAggregate(aggregateList=aggregateList,\
                interpolationList=interpolationList,interpolationSpanList=interpolationSpanList)
        elif requestType == 'getSeries':
            inlineVarPayload = [{"kind":"numeric", "value": {"tsx": "$event.value"}, "filter": None}]
            projectedVarNames = ['tagData']
        elif requestType == 'getEvents':
            projectedVarNames = None
            
        else:
            raise TSIQueryError(
                "TSIClient: Not a valid request type "
            )

        for i, _ in enumerate(timeseries):
            if timeseries[i] == None:
                logging.error("No such tag: {tag}".format(tag=colNames[i]))
                continue
            if types[i] == None:
                logging.error("Type not defined for {timeseries}".format(timeseries=timeseries[i]))
                continue
            logging.info(f'Timeseries {colNames[i]} has type {typeList[types[i]]}')
            payload = {
                requestType: {
                    "timeSeriesId": [timeseries[i]],
                    "timeSeriesName": None,
                    "searchSpan": {"from": timespan[0], "to": timespan[1]},
                    "filter": None,
                    "interval": interval,
                    "inlineVariables": {},
                    "take": 250000,
                    "projectedVariables": projectedVarNames,
                }
            }
            if requestType == 'aggregateSeries':
                if isinstance(aggregateList, list):
                    for j in range(0,len(aggregateList)):
                        payload[requestType]["inlineVariables"][projectedVarNames[j]] = inlineVarPayload[j]
                else:
                    payload[requestType]["inlineVariables"][projectedVarNames[0]] = inlineVarPayload[0]
                    
            elif requestType == 'getSeries':
                payload[requestType]["inlineVariables"][projectedVarNames[0]] = inlineVarPayload[0]
                
            elif requestType == 'getEvents':
                payload[requestType]["filter"] = {"tsx": "($event.value.Double != null) OR ($event.Status.String = 'Good')"}
                """ If this line is ignored all properties will be returned """
                payload[requestType]["projectedProperties"] = [{"name":"value", "type":"Double"}]
                
            headers = {
                "x-ms-client-application-name": self._applicationName,
                "Authorization": authorizationToken,
                "Content-Type": "application/json",
                "cache-control": "no-cache",
            }
            try:
                jsonResponse = requests.request(
                    "POST",
                    url,
                    data=json.dumps(payload),
                    headers=headers,
                    params=querystring,
                )
                jsonResponse.raise_for_status()
            except requests.exceptions.ConnectTimeout:
                logging.error("TSIClient: The request to the TSI api timed out.")
                raise
            except requests.exceptions.HTTPError:
                logging.error("TSIClient: The request to the TSI api returned an unsuccessfull status code.")
                raise

            if jsonResponse.text:
                response = json.loads(jsonResponse.text)
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
                
            if requestType == 'aggregateSeries':
                try:
                    assert i == 0
                    if isinstance(aggregateList, list):
                        for idx, agg in enumerate(aggregateList):
                            currColName = colNames[i] + "/" + agg
                            if idx == 0:
                                df = pd.DataFrame(
                                    {
                                        "timestamp": response["timestamps"],
                                        currColName : response["properties"][idx]["values"],
                                    }
                                )
                            else:
                                df[currColName] = response["properties"][idx]["values"]
                    else:
                        df = pd.DataFrame(
                                    {
                                        "timestamp": response["timestamps"],
                                        colNames[i] : response["properties"][0]["values"],
                                    }
                                )
                except:
                    if isinstance(aggregateList, list):
                        for idx, agg in enumerate(aggregateList):
                            currColName  = colNames[i] + "/" + agg
                            df[currColName] = response["properties"][idx]["values"]
                    else:
                        df[colNames[i]] = response["properties"][0]["values"]
                        
                finally:
                    logging.critical("Loaded data for tag: {tag}".format(tag=colNames[i]))
                    
            elif requestType == 'getSeries':
                result = response
                while 'continuationToken' in list(response.keys()):
                    print("continuation token found, appending")
                    headers = {
                        "x-ms-client-application-name": self._applicationName,
                        "Authorization": authorizationToken,
                        "Content-Type": "application/json",
                        "cache-control": "no-cache",
                        'x-ms-continuation': response['continuationToken'], 
                    }
                    jsonResponse = requests.request(
                        "POST",
                        url,
                        data=json.dumps(payload),
                        headers=headers,
                        params=querystring,
                    )
                    jsonResponse.raise_for_status()
                    
                    if jsonResponse.text:
                        response = json.loads(jsonResponse.text)
                    result["timestamps"].extend(response["timestamps"])
                    
                    result["properties"][0]["values"].extend(response["properties"][0]["values"])
                    
                try:
                    assert i == 0
                    df = pd.DataFrame(
                            {
                                "timestamp": result["timestamps"],
                                colNames[i] : result["properties"][0]["values"],
                            }
                        )
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.sort_values(by=['timestamp'], inplace=True)

                except:
                    df_temp = pd.DataFrame(
                        {
                            "timestamp": result["timestamps"],
                            colNames[i] : result["properties"][0]["values"],
                        }
                    )
                    df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'])
                    df_temp.sort_values(by=['timestamp'], inplace=True)
                    """ Tolerance: Limits to merge asof so there will be placed Nones if no values"""
                    df = pd.merge_asof(df,df_temp,on=['timestamp'],direction='nearest',tolerance=pd.Timedelta(seconds=30))
                finally:
                    logging.critical("Loaded data for tag: {tag}".format(tag=colNames[i]))
                    
            elif requestType == 'getEvents':
                result = response
                while 'continuationToken' in list(response.keys()):
                    print("continuation token found, appending")
                    headers = {
                        "x-ms-client-application-name": self._applicationName,
                        "Authorization": authorizationToken,
                        "Content-Type": "application/json",
                        "cache-control": "no-cache",
                        'x-ms-continuation': response['continuationToken'], 
                    }
                    jsonResponse = requests.request(
                        "POST",
                        url,
                        data=json.dumps(payload),
                        headers=headers,
                        params=querystring,
                    )
                    jsonResponse.raise_for_status()
                    if jsonResponse.text:
                        response = json.loads(jsonResponse.text)
                    result["timestamps"].extend(response["timestamps"])
                    result["properties"][0]["values"].extend(response["properties"][0]["values"])
                    
                try:
                    assert i == 0
                    df = pd.DataFrame(
                            {
                                "timestamp": result["timestamps"],
                                colNames[i] : result["properties"][0]["values"],
                            }
                        )
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.sort_values(by=['timestamp'], inplace=True)

                except:
                    df_temp = pd.DataFrame(
                        {
                            "timestamp": result["timestamps"],
                            colNames[i] : result["properties"][0]["values"],
                        }
                    )
                    df_temp['timestamp'] = pd.to_datetime(df_temp['timestamp'])
                    df_temp.sort_values(by=['timestamp'], inplace=True)
                    """ Tolerance: Limits to merge asof so there will be placed Nones if no values"""
                    df = pd.merge_asof(df,df_temp,on=['timestamp'],direction='nearest',tolerance=pd.Timedelta(seconds=30))
                finally:
                    logging.critical("Loaded data for tag: {tag}".format(tag=colNames[i]))
        return df
