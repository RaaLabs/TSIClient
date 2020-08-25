from ..authorization.authorization_api import AuthorizationApi
from ..common.common_funcs import CommonFuncs
import requests
import json
import logging


class TypesApi():
    def __init__(
        self,
        application_name: str, 
        environment_id: str,
        authorization_api: AuthorizationApi,
        common_funcs: CommonFuncs,
        instances=None,
    ):

        self.authorization_api = authorization_api
        self._applicationName = application_name
        self.environmentId = environment_id
        self.common_funcs = common_funcs
        self.instances = instances

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

        authorizationToken = self.authorization_api._getToken()

        url = "https://" + self.environmentId + ".env.timeseries.azure.com/timeseries/types"
        querystring = self.common_funcs._getQueryString()
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

    def getTypeByDescription(self, names):
        """Returns the type ids that correspond to the given descriptions.

        Args:
            names (list): The descriptions for which to get type ids.

        Returns:
            list: The type ids, None if timeseries description does not exist in the TSI environment.
        """

        if not isinstance(names,list):
            names = [names]
        typeIds=[]
        nameMap={}
        for instance in self.instances['instances']:
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

        if not isinstance(ids,list):
            ids = [ids]
        typeIds=[]
        idMap={}
        for instance in self.instances['instances']:
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

        if not isinstance(names,list):
            names = [names]
        typeIds=[]
        nameMap={}
        for instance in self.instances['instances']:
            if 'name' in instance:
                nameMap[instance['name']] = instance
        for name in names:
            if name in nameMap:
                typeIds.append(nameMap[name]['typeId'])
            else:
                typeIds.append(None)
        return typeIds

    def writeTypes(self, payload):
        jsonResponse = self._updateTimeSeries(payload, 'types')
        return jsonResponse

    def _updateTimeSeries(self, payload, timeseries):
        """Writes instances to the TSI environment.

        Args:
            payload (str): A json-serializable payload that is posted to the TSI environment.
                The format of the payload is specified in the Azure TSI documentation.

        Returns:
            dict: The response of the TSI api call.
        """

        authorizationToken = self.authorization_api._getToken()

        url = "https://{environmentId}.env.timeseries.azure.com/timeseries/{timeseries}/$batch".format(environmentId=self.environmentId,timeseries=timeseries)
        
        querystring = self.common_funcs._getQueryString()

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
