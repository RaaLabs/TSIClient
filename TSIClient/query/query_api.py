from ..authorization.authorization_api import AuthorizationApi
from ..common.common_funcs import CommonFuncs
from ..types.types_api import TypesApi
from ..exceptions import TSIQueryError, TSIStoreError
import requests
import json
import logging
import pandas as pd



class QueryApi():
    def __init__(
        self,
        application_name: str,
        environment_id: str,
        authorization_api: AuthorizationApi,
        common_funcs: CommonFuncs,
        typesApi: TypesApi,
        instances: dict,
    ):
        self.authorization_api = authorization_api
        self._applicationName = application_name
        self.environmentId = environment_id
        self.common_funcs = common_funcs
        self.instances = instances
        self.types_api = typesApi

    def _getVariableAggregate(self, typeList=None, currType=None, aggregate=None, interpolationKind=None, interpolationSpan=None):
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
        variableName = None

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
                if typeList == None or currType == None:
                    inlineVar = {"kind":"numeric", "value": {"tsx": "$event.value"}, "filter": None,\
                        "interpolation":{"kind": "{0!s}".format(interpolationKind),\
                        "boundary":{"span": "{0!s}".format(interpolationSpan)}},\
                        "aggregation": {"tsx": "{0!s}($value)".format(aggregate)}}
                else:
                    inlineVar = {"kind":"numeric", "value": {"tsx": typeList[currType]}, "filter": None,\
                        "interpolation":{"kind": "{0!s}".format(interpolationKind),\
                        "boundary":{"span": "{0!s}".format(interpolationSpan)}},\
                        "aggregation": {"tsx": "{0!s}($value)".format(aggregate)}}


            else: 
                variableName = aggregate.capitalize() + 'VarAggregate'
                if typeList == None or currType == None:
                    inlineVar = {"kind":"numeric", "value": {"tsx": "$event.value"}, "filter": None,\
                        "aggregation": {"tsx": "{0!s}($value)".format(aggregate)}}
                else:
                    inlineVar = {"kind":"numeric", "value": {"tsx": typeList[currType]}, "filter": None,\
                        "aggregation": {"tsx": "{0!s}($value)".format(aggregate)}}

        return (inlineVar, variableName)

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

    def getInlineVariablesAggregate(self, typeList=None, currType=None, aggregateList=None, interpolationList=None, interpolationSpanList=None):
        """Returns a tuple of lists to apply in the payload consisiting of the InlineVariables and the 
            projectedVariables.
        Args:
            aggregateList (list): List of the aggregation methods to be used without interpolation:
                ("min", "max", "sum", "avg", "first", "last", "median", "stdev").
                The aggregation method to be used with interpolation:
                ("twsum", "twavg", "left", "right")
            interpolationList (list): A list of interpolation methods. Either Linear or Step.
            interpolationSpanList (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interpolation Boundary span ="P1D", for 1 day to the left and right of the search span to be used for Interpolation.
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
                (inlineVar, variableName) = self._getVariableAggregate(typeList=typeList, currType=currType, aggregate=aggregateList[i],\
                    interpolationKind=interpolationList[i], interpolationSpan=interpolationSpanList[i])
                projectedVarNames.append(variableName)
                inlineVarPayload.append(inlineVar)

        elif isinstance(aggregateList, str) and (isinstance(interpolationList, str) or interpolationList == None)\
            and (isinstance(interpolationSpanList, str) or interpolationSpanList == None):

            projectedVarNames = []
            inlineVarPayload = []
            (inlineVar, variableName) = self._getVariableAggregate(typeList=typeList, currType=currType, aggregate=aggregateList,\
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

        if not isinstance(ids,list):
            ids = [ids]
        timeSeriesNames=[]
        idMap={}
        for instance in self.instances['instances']:
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

        timeSeriesIds=[]
        nameMap={}
        for instance in self.instances['instances']:
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

        if not isinstance(names,list):
            names = [names]
        timeSeriesIds=[]
        nameMap={}
        for instance in self.instances['instances']:
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

        if not isinstance(names,list):
            names = [names]
        timeSeriesIds=[]
        nameMap={}
        for instance in self.instances['instances']:
            if 'description' in instance:
                nameMap[instance['description']] = instance
        for name in names:
            if name in nameMap:
                timeSeriesIds.append(nameMap[name]['timeSeriesId'][0])
            else:
                timeSeriesIds.append(None)
        return timeSeriesIds


    def getDataByName(
        self,
        variables,
        timespan,
        interval,
        aggregateList=None,
        interpolationList=None,
        interpolationSpanList=None,
        requestBodyType=None,
        useWarmStore=False
    ):
        """Returns a dataframe with timestamps and values for the time series names given in "variables".

        Can be used to return data for single and multiple timeseries. Names must be exact matches.

        Args:
            variables (list): The variable names. Corresponds to the "name/Time Series Name" field of the time series instances.
            timespan (list): A list of two timestamps. First list element ist the start time, second element is the end time.
                Example: timespan=['2019-12-12T15:35:11.68Z', '2019-12-12T17:02:05.958Z']
            interval (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interval="PT1M", for 1 minute aggregation.
            aggregateList (list): List of the aggregation methods to be used without interpolation:
                ("min", "max", "sum", "avg", "first", "last", "median", "stdev").
                The aggregation method to be used with interpolation:
                ("twsum", "twavg", "left", "right")
            interpolationList (list): A list of interpolation methods. Either "Linear" or "Step".
            interpolationSpanList (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interpolation Boundary span ="P1D", for 1 day to the left and right of the search span to be used for Interpolation.
            requestBodyType (str): Type of the request, either "getSeries", "aggregateSeries" or "getEvents".
            useWarmStore (bool): If True, the query is executed on the warm storage (free of charge), otherwise on the cold storage. Defaults to False.

        Returns:
            A pandas dataframe with timeseries data. Columns are ordered the same way as the variable names.

        Raises:
            TSIStoreError: Raised if the was tried to execute on the warm store, but the warm store is not enabled.
            TSIQueryError: Raised if there was an error in the query arguments (e.g. wrong formatting).

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> data = client.query.getDataByName(
            ...     variables=["timeseries_name_1", "timeseries_name_2"],
            ...     timespan=["2020-01-25T10:00:11.68Z", "2020-01-26T13:45:11.68Z"],
            ...     interval="PT5M",
            ...     aggregateList=["avg"],
            ...     useWarmStore=False
            ... )
        """

        authorizationToken = self.authorization_api._getToken()
        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = self.common_funcs._getQueryString(useWarmStore=useWarmStore)
        timeseries = self.getIdByName(variables)
        types = self.types_api.getTypeByName(variables)

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


    def getDataByDescription(
        self,
        variables,
        TSName,
        timespan,
        interval,
        aggregateList=None,
        interpolationList=None,
        interpolationSpanList=None,
        requestBodyType=None,
        useWarmStore=False
    ):
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
            aggregateList (list): List of the aggregation methods to be used without interpolation:
                ("min", "max", "sum", "avg", "first", "last", "median", "stdev").
                The aggregation method to be used with interpolation:
                ("twsum", "twavg", "left", "right")
            interpolationList (list): A list of interpolation methods. Either "Linear" or "Step".
            interpolationSpanList (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interpolation Boundary span ="P1D", for 1 day to the left and right of the search span to be used for Interpolation.
            requestBodyType (str): Type of the request, either "getSeries", "aggregateSeries" or "getEvents".
            useWarmStore (bool): If True, the query is executed on the warm storage (free of charge), otherwise on the cold storage. Defaults to False.

        Returns:
            A pandas dataframe with timeseries data. Columns are ordered the same way as the variable descriptions.

        Raises:
            TSIStoreError: Raised if the was tried to execute on the warm store, but the warm store is not enabled.
            TSIQueryError: Raised if there was an error in the query arguments (e.g. wrong formatting).

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> data = client.query.getDataByDescription(
            ...     variables=["timeseries_description_1", "timeseries_description_2"],
            ...     TSName=["my_timeseries_name_1", "my_timeseries_name_2"]
            ...     timespan=["2020-01-25T10:00:11.68Z", "2020-01-26T13:45:11.68Z"],
            ...     interval="PT5M",
            ...     aggregateList=["avg"],
            ...     useWarmStore=False
            ... )
        """

        authorizationToken = self.authorization_api._getToken()
        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = self.common_funcs._getQueryString(useWarmStore=useWarmStore)
        timeseries = self.getIdByDescription(variables)
        types = self.types_api.getTypeByDescription(variables)

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


    def getDataById(
        self,
        timeseries,
        timespan,
        interval,
        aggregateList=None,
        interpolationList=None,
        interpolationSpanList=None,
        requestBodyType=None,
        useWarmStore=False
    ):
        """Returns a dataframe with timestamp and values for the time series that match the description given in "timeseries".

        Can be used to return data for single and multiple timeseries. Timeseries ids must be an exact matches.

        Args:
            timeseries (list): The timeseries ids. Corresponds to the "timeSeriesId" field of the time series instances.
            timespan (list): A list of two timestamps. First list element ist the start time, second element is the end time.
                Example: timespan=['2019-12-12T15:35:11.68Z', '2019-12-12T17:02:05.958Z']
            interval (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interval="PT1M", for 1 minute aggregation. If "aggregate" is None, the raw events are returned.
            aggregateList (list): List of the aggregation methods to be used without interpolation:
                ("min", "max", "sum", "avg", "first", "last", "median", "stdev").
                The aggregation method to be used with interpolation:
                ("twsum", "twavg", "left", "right")
            interpolationList (list): A list of interpolation methods. Either "Linear" or "Step".
            interpolationSpanList (str): The time interval that is used during aggregation. Must follow the ISO-8601 duration format.
                Example: interpolation Boundary span ="P1D", for 1 day to the left and right of the search span to be used for Interpolation.
            requestBodyType (str): Type of the request, either "getSeries", "aggregateSeries" or "getEvents".
            useWarmStore (bool): If True, the query is executed on the warm storage (free of charge), otherwise on the cold storage. Defaults to False.

        Returns:
            A pandas dataframe with timeseries data. Columns are ordered the same way as the timeseries ids.

        Raises:
            TSIStoreError: Raised if the was tried to execute on the warm store, but the warm store is not enabled.
            TSIQueryError: Raised if there was an error in the query arguments (e.g. wrong formatting).

        Example:
            >>> from TSIClient import TSIClient as tsi
            >>> client = tsi.TSIClient()
            >>> data = client.query.getDataById(
            ...     timeseries=["timeseries_id_1", "timeseries_id_2"],
            ...     timespan=["2020-01-25T10:00:11.68Z", "2020-01-26T13:45:11.68Z"],
            ...     interval="PT5M",
            ...     aggregateList=["avg"],
            ...     useWarmStore=False
            ... )
        """

        authorizationToken = self.authorization_api._getToken()
        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = self.common_funcs._getQueryString(useWarmStore=useWarmStore)
        types = self.types_api.getTypeById(timeseries)

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
        df = pd.DataFrame()
        typeList = self.types_api.getTypeTsx()
        if not isinstance(types,list):
            types = [types]
        if not isinstance(timeseries,list):
            timeseries=[timeseries]

        if otherColNamesThanTimeseriesIds != None:
            colNames = otherColNamesThanTimeseriesIds
        else:
            colNames = timeseries
        

        for i, _ in enumerate(timeseries):
            if timeseries[i] == None:
                logging.error("No such tag: {tag}".format(tag=colNames[i]))
                continue
            if types[i] == None:
                logging.error("Type not defined for {timeseries}".format(timeseries=timeseries[i]))
                continue
            logging.info(f'Timeseries {colNames[i]} has type {typeList[types[i]]}')
            if requestType == 'aggregateSeries':
                (inlineVarPayload, projectedVarNames) = self.getInlineVariablesAggregate(typeList=typeList,currType=types[i], aggregateList=aggregateList,\
                    interpolationList=interpolationList,interpolationSpanList=interpolationSpanList)
            elif requestType == 'getSeries':
                inlineVarPayload = [{"kind":"numeric", "value": {"tsx": typeList[types[i]]}, "filter": None}]
                projectedVarNames = ['tagData']
            elif requestType == 'getEvents':
                projectedVarNames = None

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

            response = json.loads(jsonResponse.text)
            if "error" in response:
                if "innerError" in response["error"]:
                    if response["error"]["innerError"]["code"] == "TimeSeriesQueryNotSupported":
                        raise TSIStoreError(
                            "TSIClient: Warm store not enabled in TSI environment: {id}. Set useWarmStore to False."
                                .format(id=self.environmentId),
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
                    if df.empty:
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
                    else:
                        if isinstance(aggregateList, list):
                            for idx, agg in enumerate(aggregateList):
                                currColName  = colNames[i] + "/" + agg
                                df[currColName] = response["properties"][idx]["values"]
                        else:
                            df[colNames[i]] = response["properties"][0]["values"]

                finally:
                    logging.critical("Loaded data for tag: {tag}".format(tag=colNames[i]))

            else:
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
                    if df.empty:
                        df = pd.DataFrame(
                                {
                                    "timestamp": result["timestamps"],
                                    colNames[i] : result["properties"][0]["values"],
                                }
                            )
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        df.sort_values(by=['timestamp'], inplace=True)
                    else:
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