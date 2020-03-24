# TSIClient
[![Build Status](https://dev.azure.com/raalabs/One%20Operation%20Analytics%20Serving/_apis/build/status/RaaLabs.TSIClient?branchName=master)](https://dev.azure.com/raalabs/One%20Operation%20Analytics%20Serving/_build/latest?definitionId=8&branchName=master)
[![Documentation Status](https://readthedocs.org/projects/tsiclient/badge/?version=latest)](https://tsiclient.readthedocs.io/en/latest/?badge=latest)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d73dafb558f64d8580a4a87517c32340)](https://www.codacy.com/manual/rafaelschlatter/TSIClient?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=rafaelschlatter/TSIClient&amp;utm_campaign=Badge_Grade)

The TSIClient is a Python SDK for Microsoft Azure time series insights. It provides methods to conveniently retrieve your data and is designed
for analysts, data scientists and developers working with time series data in Azure TSI.

## Documentation
- Azure time series REST APIs: <https://docs.microsoft.com/en-us/rest/api/time-series-insights/>
- TSIClient: <https://tsiclient.readthedocs.io/en/latest/>

## Installation
The TSIClient is not yet on PyPi, but you can install it directly from GitHub:

````bash
pip install git+https://github.com/RaaLabs/TSIClient.git
````
## Quickstart
Instantiate the TSIClient to query your TSI environment. Use the credentials from your service principal in Azure that has access to the TSI environment.

````python
from TSIClient import TSIClient as tsi

client = tsi.TSIClient(
    enviroment="<your-tsi-env-name>",
    client_id="<your-client-id>",
    client_secret="<your-client-secret>",
    tenant_id="<your-tenant-id>",
    applicationName="<your-app-name>">
)
````

You can query your timeseries data by timeseries id, timeseries name or timeseries description. The Microsoft TSI apis support aggregation, so you can specify a sampling freqency and an aggregation method. Refer to the documentation for detailed information.

````python
data = client.getDataById(
    timeseries=["timeseries_id1", "timeseries_id2"],
    timespan=["2019-12-12T15:35:11.68Z", "2019-12-12T17:02:05.958Z"],
    interval="PT5M",
    aggregate="avg",
    use_warm_store=False
)
````

This returns a pandas dataframe, which can be used for analysis.

## Contributing
Contributions are welcome. See the [developer reference](docs/source/developer.rst) for details.

## License
TSIClient is licensed under the MIT license. See [LICENSE](LICENSE.txt) file for details.
