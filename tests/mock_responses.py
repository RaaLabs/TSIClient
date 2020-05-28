class MockURLs():
    """This class holds mock urls that can be used to mock requests to the TSI environment.
    Note that there are dependencies between the MockURLs, the MockResponses and the parameters used
    in "create_TSICLient".
    """

    oauth_url = "https://login.microsoftonline.com/{}/oauth2/token".format("yet_another_tenant_id")
    env_url = "https://api.timeseries.azure.com/environments"
    hierarchies_url = "https://{}.env.timeseries.azure.com/timeseries/hierarchies".format("00000000-0000-0000-0000-000000000000")
    types_url = "https://{}.env.timeseries.azure.com/timeseries/types".format("00000000-0000-0000-0000-000000000000")
    instances_url = "https://{}.env.timeseries.azure.com/timeseries/instances/".format("00000000-0000-0000-0000-000000000000")
    environment_availability_url = "https://{}.env.timeseries.azure.com/availability".format("00000000-0000-0000-0000-000000000000")
    query_getseries_url = "https://{}.env.timeseries.azure.com/timeseries/query?".format("00000000-0000-0000-0000-000000000000")


class MockResponses():
    """This class holds mocked request responses which can be used across tests.
    The first json responses are taken from the official Azure TSI api documentation.
    """
    mock_types = {
        "types": [
            {
                "id": "1be09af9-f089-4d6b-9f0b-48018b5f7393",
                "name": "DefaultType",
                "description": "My Default type",
                "variables": {
                    "EventCount": {
                    "kind": "aggregate",
                    "filter": None,
                    "aggregation": {
                        "tsx": "count()"
                        }
                    }       
                }
            },
            {
                "id": "1be09af9-f089-4d6b-9f0b-48018b5f7393",
                "name": "Double",
                "description": "My Default type",
                "variables": {
                    "Value": {
                        "kind": "numeric",
                        "value": {
                            "tsx":"$event.[value].Double"
                        },
                        "aggregation": {
                            "tsx": "avg($value)"
                        }
                    }
                }
            }
        ],
        "continuationToken": "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="
    }

    mock_hierarchies = {
        "hierarchies": [
            {
                "id": "6e292e54-9a26-4be1-9034-607d71492707",
                "name": "Location",
                "source": {
                    "instanceFieldNames": [
                    "state",
                    "city"
                    ]
                }
            }
        ],
        "continuationToken": "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="
    }

    mock_oauth = {
        "token_type": "some_type",
        "access_token": "token"
    }

    mock_environments = {
        "environments": [
            {
                "displayName":"Test_Environment",
                "environmentFqdn": "00000000-0000-0000-0000-000000000000.env.timeseries.azure.com",
                "environmentId": "00000000-0000-0000-0000-000000000000",
                "resourceId": "resourceId"
            }
        ]
    }

    mock_instances = {
        "instances": [
            {
                "typeId": "1be09af9-f089-4d6b-9f0b-48018b5f7393",
                "name": "F1W7.GS1",
                "timeSeriesId": [
                    "006dfc2d-0324-4937-998c-d16f3b4f1952",
                    "T1"
                ],
                "description": "ContosoFarm1W7_GenSpeed1",
                "hierarchyIds": [
                    "33d72529-dd73-4c31-93d8-ae4e6cb5605d"
                ],
                "instanceFields": {
                    "Name": "GeneratorSpeed",
                    "Plant": "Contoso Plant 1",
                    "Unit": "W7",
                    "System": "Generator System"
                }
            }
        ],
        "continuationToken": "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="
    }

    mock_environment_availability = {
        "availability": {
            "intervalSize": "PT1H",
            "distribution": {
                "2019-03-27T04:00:00Z": 432447,
                "2019-03-27T05:00:00Z": 432340,
                "2019-03-27T06:00:00Z": 432451,
                "2019-03-27T07:00:00Z": 432436,
                "2019-03-26T13:00:00Z": 386247,
                "2019-03-27T00:00:00Z": 436968,
                "2019-03-27T01:00:00Z": 432509,
                "2019-03-27T02:00:00Z": 432487
            },
            "range": {
                "from": "2019-03-14T06:38:27.153Z",
                "to": "2019-03-27T03:57:11.697Z"
            }
        }
    }

    mock_query_getseries_success = {
        "timestamps": [
            "2016-08-01T00:00:10Z",
            "2016-08-01T00:00:11Z",
            "2016-08-01T00:00:12Z",
            "2016-08-01T00:00:13Z",
            "2016-08-01T00:00:14Z",
            "2016-08-01T00:00:15Z",
            "2016-08-01T00:00:16Z",
            "2016-08-01T00:00:17Z",
            "2016-08-01T00:00:18Z",
            "2016-08-01T00:00:19Z",
            "2016-08-01T00:00:20Z"
        ],
        "properties": [
            {
                "name": "AverageTest",
                "type": "Double",
                "values": [
                    65.125,
                    65.375,
                    65.625,
                    65.875,
                    66.125,
                    66.375,
                    66.625,
                    66.875,
                    67.125,
                    67.375,
                    67.625
                ]
            }
        ],
        "progress": 50,
        "continuationToken": "aXsic2tpcCI6MTAxYZwidGFrZSI6MTAwMH0="
    }

    mock_query_getseries_tsistoreerror = {
        "error" : {
            "code" : "...",
            "message" : "...",
            "innerError" : {  
                "code" : "TimeSeriesQueryNotSupported",
                "message" : "...",
            }
        }
    }

    mock_query_getseries_tsiqueryerror = {
        "error" : {
            "code" : "...",
            "message" : "...",
        }
    }
