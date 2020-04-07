Userguide
==========
The example in this userguide walks you through a typical workflow with the
TSIClient.

.. note::
    We highly recommend to familiarize yourself with the Microsoft
    Azure Time Series Insights (TSI) documentation and especially its TSI
    APIs. Find it here: https://docs.microsoft.com/en-us/rest/api/time-series-insights/.


First you have to instantiate the TSIClient. Authentication works through the use of
a service principal. When you create the service principal in Azure, make sure to
note down the details, since you need them to use the TSIClient. Note that one instance
of the TSIClient can only connect to one TSI environment, if you wish to connect to
multiple TSI environments, you need to create multiple instances of the TSIClient.

.. code-block:: python

    >>> from TSIClient import TSIClient as tsi
    >>> client = tsi.TSIClient(
    ...     enviroment="<your-tsi-env-name>",
    ...     client_id="<your-client-id>",
    ...     client_secret="<your-client-secret>",
    ...     tenant_id="<your-tenant-id>",
    ...     applicationName="<your-app-name>">
    ... )

Alternatively, you can use environment variables to instantiate the TSIClient.
Set the following variables (commands for Mac/Linux):

.. code-block:: console

    $ export TSICLIENT_APPLICATION_NAME=<your-app-name>
    $ export TSICLIENT_ENVIRONMENT_NAME=<your-tsi-env-name>
    $ export TSICLIENT_CLIENT_ID=<your-client-id>
    $ export TSICLIENT_CLIENT_SECRET=<your-client-secret>
    $ export TSICLIENT_TENANT_ID=<your-tenant-id>

Now you can instantiate the TSIClient without passing any arguments. Be aware
that the environment variables take precedence over the arguments.

.. code-block:: python

    >>> from TSIClient import TSIClient as tsi
    >>> client = tsi.TSIClient()

You can verify that the TSIClient is pointing at the right TSI environment by running the
following command. It returns the environment id, which you can compare with your data
access FQDN (you find it on the overview on your TSI environment page in Azure).

.. code-block:: python

    >>> client.getEnviroment()
    'azbf6395-3459-143u-j931-6io92e473892'


You can query data by timeseries id, name or description. The TSIClient has several methods
to make qyering data easy. Use ``getInstances()`` to retrieve all timeseries instances.

.. code-block:: python

    >>> response = client.getInstances()
    >>> print(response["instances"][0])
    {"instances": [{"typeId": "9b84e946-7b36-4aa0-9d26-71bf48cb2aff", "name": "F1W7.GS1",
    "timeSeriesId": ["006dfc2d-0324-4937-998c-d16f3b4f1952", "T1"], "description": "ContosoFarm1W7_GenSpeed1",
    "hierarchyIds": ["33d72529-dd73-4c31-93d8-ae4e6cb5605d"], "instanceFields": {
    "Name": "GeneratorSpeed", "Plant": "Contoso Plant 1", "Unit": "W7", "System": "Generator System"}}],
    "continuationToken": "aXsic2tpcCI6MTAwMCwidGFrZSI6MTAwMH0="}


You can also get the timeseries id if you have the timeseries name (it is possible to specify
multiple names, which returns multiple ids). The methods ``getIdByDescription()``
and ``getNameById()`` work accordingly.

.. code-block:: python

    >>> client.getIdByName(["GeneratorSpeed"])
    ['006dfc2d-0324-4937-998c-d16f3b4f1952']


We recommend to query data by id with ``getDataById()``, as this is the identifier of the timeseries
that is the least likely to change. You can also retrieve data by name with ``getDataByName()``
and by description with ``getDataByDescription()``. These methods return a pandas dataframe, which
is convenient for further statistical analysis.

.. code-block:: python

    >>> data = client.getDataById(
    ...     timeseries=["timeseries_id1", "timeseries_id2"],
    ...     timespan=["2020-01-25T10:00:11.68Z", "2020-01-26T13:45:11.68Z"],
    ...     interval="PT5M",
    ...     aggregate="avg",
    ...     useWarmStore=False
    ... )
    >>> data
    timestamp                   timeseries_id1  timeseries_id2
    0    2020-01-25T10:00:00Z       360.272727      242.692308
    1    2020-01-25T10:05:00Z       362.588235      244.523810
    2    2020-01-25T10:10:00Z       369.280000      245.000000
    3    2020-01-25T10:15:00Z       365.952381      242.962963
    4    2020-01-25T10:20:00Z       367.962963      241.391304
    ..                    ...              ...             ...
    329  2020-01-26T13:25:00Z       315.210526      299.250000
    330  2020-01-26T13:30:00Z       310.060606      569.776119
    331  2020-01-26T13:35:00Z       300.961538      299.000000
    332  2020-01-26T13:40:00Z       301.645161      293.421053
    333  2020-01-26T13:45:00Z       300.000000             NaN

    [334 rows x 3 columns]
