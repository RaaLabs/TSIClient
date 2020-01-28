User guide
==========
The example in this user guide walks you through a typical workflow with the
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
        ... enviroment="<your-tsi-env-name>",
        ... client_id="<your-client-id>",
        ... client_secret="<your-client-secret>",
        ... tenant_id="<your-tenant-id>",
        ... applicationName="<your-app-name>">
    ... )

You can verify that the TSIClient is pointing at the right TSI environment by running the
following command. It returns the environment id, which you can compare with your data
access FQDN (you find it on the overview on your TSI environment page in Azure).

.. code-block:: python

    >>> client.getEnviroment()
    azbf6395-3459-143u-j931-6io92e473892

You can query data by timeseries id, name or description. 