# TSIClient
[![build](https://github.com/RaaLabs/TSIClient/workflows/Python%20CI/badge.svg)](https://github.com/RaaLabs/TSIClient/actions)
[![codecov](https://codecov.io/gh/RaaLabs/TSIClient/branch/master/graph/badge.svg?token=8qlhjGp0Yu)](https://codecov.io/gh/RaaLabs/TSIClient)
[![Documentation Status](https://readthedocs.org/projects/raalabs-tsiclient/badge/?version=latest)](https://raalabs-tsiclient.readthedocs.io/en/latest/?badge=latest)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/e4ddf7b091dc4d4ebbe3bcd572487f02)](https://www.codacy.com/gh/RaaLabs/TSIClient?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RaaLabs/TSIClient&amp;utm_campaign=Badge_Grade)
[![PyPI version](https://badge.fury.io/py/TSIClient.svg)](https://badge.fury.io/py/TSIClient)
[![Downloads](https://pepy.tech/badge/tsiclient/month)](https://pepy.tech/project/tsiclient)

The TSIClient is a Python SDK for Microsoft Azure time series insights. It provides methods to conveniently retrieve your data and is designed
for analysts, data scientists and developers working with time series data in Azure TSI.

## Documentation
- Azure time series REST APIs: <https://docs.microsoft.com/en-us/rest/api/time-series-insights/>
- TSIClient: <https://raalabs-tsiclient.readthedocs.io/en/latest/>

## Installation
We recommended to use a Python version >= 3.6. You can install the TSIClient from PyPi:
````bash
pip install TSIClient
````
Or if you want to get the latest code, directly fom GitHub:

````bash
pip install git+https://github.com/RaaLabs/TSIClient.git
````
## Quickstart
Instantiate the TSIClient to query your TSI environment. Use the credentials from your service principal in Azure that has access to the TSI environment (you can also use environment variables to instantiate the TSIClient or provide a specific TSI api version, check the documentation).

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
data = client.query.getDataById(
    timeseries=["timeseries_id1", "timeseries_id2"],
    timespan=["2019-12-12T15:35:11.68Z", "2019-12-12T17:02:05.958Z"],
    interval="PT5M",
    aggregate="avg",
    useWarmStore=False
)
````

This returns a pandas dataframe, which can be used for analysis.

## Contributing
Contributions are welcome. See the [developer reference](docs/source/developer.rst) for details.

## License
TSIClient is licensed under the MIT license. See [LICENSE](LICENSE.txt) file for details.
