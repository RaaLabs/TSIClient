import pytest
import requests
import pandas as pd
from TSIClient.exceptions import TSIQueryError, TSIStoreError
from tests.mock_responses import MockURLs, MockResponses


class TestQueryApi:
    def test__getVariableAggregate_with_no_aggregate_returns_None_and_getSeries(
        self, client
    ):
        requestType = client.query.getRequestType(aggregate=None)

        assert requestType == "getSeries"

    def test__getVariableAggregate_with_unsupported_aggregate_raises_TSIQueryError(
        self, client, caplog
    ):
        with pytest.raises(TSIQueryError):
            client.query._getVariableAggregate(aggregate="unsupported_aggregate")

    def test__getVariableAggregate_with_avg_aggregate_returns_aggregate_and_aggregateSeries(
        self, client
    ):
        inlineVar, variableName = client.query._getVariableAggregate(
            aggregate="avg", interpolationKind=None, interpolationSpan=None
        )

        assert inlineVar == {
            "kind": "numeric",
            "value": {"tsx": "$event.value"},
            "filter": None,
            "aggregation": {"tsx": "avg($value)"},
        }
        assert isinstance(inlineVar, dict)
        assert variableName == "AvgVarAggregate"

    def test_getNameById_with_one_correct_id_returns_correct_name(self, client):
        timeSeriesNames = client.query.getNameById(
            ids=["006dfc2d-0324-4937-998c-d16f3b4f1952", "made_up_id"]
        )

        assert len(timeSeriesNames) == 2
        assert timeSeriesNames[0] == "F1W7.GS1"
        assert timeSeriesNames[1] == None

    def test_getIdByAssets_with_one_existing_asset_returns_correct_id(self, client):
        timeSeriesIds = client.query.getIdByAssets(asset="F1W7")

        assert len(timeSeriesIds) == 1
        assert timeSeriesIds[0] == "006dfc2d-0324-4937-998c-d16f3b4f1952"

    def test_getIdByAssets_with_non_existant_assets_returns_empty_list(self, client):
        timeSeriesIds = client.query.getIdByAssets(asset="made_up_asset_name")

        assert len(timeSeriesIds) == 0

    def test_getIdByName_with_one_correct_name_returns_correct_id(self, client):
        timeSeriesIds = client.query.getIdByName(names=["F1W7.GS1", "made_up_name"])

        assert len(timeSeriesIds) == 2
        assert timeSeriesIds[0] == "006dfc2d-0324-4937-998c-d16f3b4f1952"
        assert timeSeriesIds[1] == None

    def test_getIdByDescription_with_one_correct_description_returns_correct_id(
        self, client
    ):
        timeSeriesIds = client.query.getIdByDescription(
            names=["ContosoFarm1W7_GenSpeed1", "made_up_description"]
        )

        assert len(timeSeriesIds) == 2
        assert timeSeriesIds[0] == "006dfc2d-0324-4937-998c-d16f3b4f1952"
        assert timeSeriesIds[1] == None

    def test_getDataById_returns_data_as_dataframe(self, client, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_success,
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        data_by_id = client.query.getDataById(
            timeseries=["006dfc2d-0324-4937-998c-d16f3b4f1952"],
            timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
            interval="PT1S",
            aggregateList="avg",
            useWarmStore=False,
        )

        assert isinstance(data_by_id, pd.DataFrame)
        assert "timestamp" in data_by_id.columns
        assert "006dfc2d-0324-4937-998c-d16f3b4f1952" in data_by_id.columns
        assert 11 == data_by_id.shape[0]
        assert 2 == data_by_id.shape[1]
        assert data_by_id.at[5, "timestamp"] == "2016-08-01T00:00:15Z"
        assert data_by_id.at[5, "006dfc2d-0324-4937-998c-d16f3b4f1952"] == 66.375

    def test_getDataById_raises_TSIStoreError(self, client, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsistoreerror,
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        with pytest.raises(TSIStoreError):
            client.query.getDataById(
                timeseries=["006dfc2d-0324-4937-998c-d16f3b4f1952"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregateList="avg",
                useWarmStore=True,
            )

    def test_getDataById_raises_TSIQueryError(self, client, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsiqueryerror,
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        with pytest.raises(TSIQueryError):
            data_by_id = client.query.getDataById(
                timeseries=["006dfc2d-0324-4937-998c-d16f3b4f1952"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregateList="avg",
                useWarmStore=False,
            )

    def test_getDataById_raises_HTTPError(self, client, requests_mock):
        requests_mock.request(
            "POST", MockURLs.query_getseries_url, exc=requests.exceptions.HTTPError
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        with pytest.raises(requests.exceptions.HTTPError):
            data_by_id = client.query.getDataById(
                timeseries=["006dfc2d-0324-4937-998c-d16f3b4f1952"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregateList="avg",
                useWarmStore=False,
            )

    def test_getDataById_raises_ConnectTimeout(self, client, requests_mock):
        requests_mock.request(
            "POST", MockURLs.query_getseries_url, exc=requests.exceptions.ConnectTimeout
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        with pytest.raises(requests.exceptions.ConnectTimeout):
            data_by_id = client.query.getDataById(
                timeseries=["006dfc2d-0324-4937-998c-d16f3b4f1952"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregateList="avg",
                useWarmStore=False,
            )

    def test_getDataByDescription_returns_data_as_dataframe(
        self, client, requests_mock
    ):
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_success,
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        data_by_description = client.query.getDataByDescription(
            variables=[
                "ContosoFarm1W7_GenSpeed1",
                "DescriptionOfNonExistantTimeseries",
            ],
            TSName=["MyTimeSeriesName", "NameOfNonExistantTimeSeries"],
            timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
            interval="PT1S",
            aggregateList="avg",
            useWarmStore=False,
        )

        assert isinstance(data_by_description, pd.DataFrame)
        assert "timestamp" in data_by_description.columns
        assert "MyTimeSeriesName" in data_by_description.columns
        assert "NameOfNonExistantTimeSeries" not in data_by_description.columns
        assert 11 == data_by_description.shape[0]
        assert 2 == data_by_description.shape[1]
        assert data_by_description.at[5, "timestamp"] == "2016-08-01T00:00:15Z"
        assert data_by_description.at[5, "MyTimeSeriesName"] == 66.375

    def test_getDataByDescription_raises_TSIStoreError(self, client, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsistoreerror,
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        with pytest.raises(TSIStoreError):
            client.query.getDataByDescription(
                variables=[
                    "ContosoFarm1W7_GenSpeed1",
                    "DescriptionOfNonExistantTimeseries",
                ],
                TSName=["MyTimeSeriesName", "NameOfNonExistantTimeSeries"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregateList="avg",
                useWarmStore=True,
            )

    def test_getDataByDescription_raises_TSIQueryError(self, client, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsiqueryerror,
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        with pytest.raises(TSIQueryError):
            data_by_description = client.query.getDataByDescription(
                variables=[
                    "ContosoFarm1W7_GenSpeed1",
                    "DescriptionOfNonExistantTimeseries",
                ],
                TSName=["MyTimeSeriesName", "NameOfNonExistantTimeSeries"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregateList="avg",
                useWarmStore=False,
            )

    def test_getDataByName_returns_data_as_dataframe(self, client, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_success,
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        data_by_name = client.query.getDataByName(
            variables=["F1W7.GS1", "NameOfNonExistantTimeseries"],
            timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
            interval="PT1S",
            aggregateList="avg",
            useWarmStore=False,
        )

        assert isinstance(data_by_name, pd.DataFrame)
        assert "timestamp" in data_by_name.columns
        assert "F1W7.GS1" in data_by_name.columns
        assert "NameOfNonExistantTimeSeries" not in data_by_name.columns
        assert 11 == data_by_name.shape[0]
        assert 2 == data_by_name.shape[1]
        assert data_by_name.at[5, "timestamp"] == "2016-08-01T00:00:15Z"
        assert data_by_name.at[5, "F1W7.GS1"] == 66.375

    def test_getDataByName_raises_TSIStoreError(self, client, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsistoreerror,
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        with pytest.raises(TSIStoreError):
            client.query.getDataByName(
                variables=["F1W7.GS1", "NameOfNonExistantTimeseries"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregateList="avg",
                useWarmStore=True,
            )

    def test_getDataByName_raises_TSIQueryError(self, client, requests_mock):
        requests_mock.request(
            "POST",
            MockURLs.query_getseries_url,
            json=MockResponses.mock_query_getseries_tsiqueryerror,
        )
        requests_mock.request("GET", MockURLs.types_url, json=MockResponses.mock_types)

        with pytest.raises(TSIQueryError):
            data_by_name = client.query.getDataByName(
                variables=["F1W7.GS1", "NameOfNonExistantTimeseries"],
                timespan=["2016-08-01T00:00:10Z", "2016-08-01T00:00:20Z"],
                interval="PT1S",
                aggregateList="avg",
                useWarmStore=False,
            )
