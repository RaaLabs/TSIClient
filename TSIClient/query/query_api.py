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

        authorizationToken = self.authorization_api._getToken()
        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = self.common_funcs._getQueryString(useWarmStore=useWarmStore)
        timeseries = self.getIdByName(variables)
        types = self.types_api.getTypeByName(variables)
        print(types)
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

        authorizationToken = self.authorization_api._getToken()
        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = self.common_funcs._getQueryString(useWarmStore=useWarmStore)
        timeseries = self.getIdByDescription(variables)
        types = self.types_api.getTypeByDescription(variables)
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

        authorizationToken = self.authorization_api._getToken()
        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/query?"
        querystring = self.common_funcs._getQueryString(useWarmStore=useWarmStore)
        aggregate, requestType = self._getVariableAggregate(aggregate=aggregate)
        types = self.types_api.getTypeById(timeseries)

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
                                    .format(id=self._environmentName),
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
