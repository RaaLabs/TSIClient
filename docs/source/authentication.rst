Authentication with Azure
=========================

We recommend to use your Azure account to authenticate with the TSIClient. For that you need to install
the Azure CLI. Follow the docs here: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli

Log in to Azure with the following command:

.. code-block:: console

    $ az login --tenant <your-azure-tenant-id>

You can find your Azure tenant id by opening the Azure portal > Azure active directory > Overview.
If you are unable to see your tenant id in AAD, ask your Azure adminstrator for the tenant id.

Once you successfully logged into Azure using the Azure CLI, you can instantiate the TSIClient, you only need to provide
the `applicationName` and `environment` arguments: 

.. code-block:: python

    >>> from TSIClient import TSIClient as tsi
    >>> client = tsi.TSIClient(
    ...     enviroment="<your-tsi-env-name>",
    ...     applicationName="<your-app-name>">,
    ...     api_version="2020-07-31"
    ... )


.. warning::
    Since version 2.1.0 authentication is preferrably done using a `DefaultAzureCredential` from the `azure-identity` package.
    Authentication by providing constructor arguments or defining these environment variables: `TSICLIENT_CLIENT_ID`, `TSICLIENT_CLIENT_SECRET`, `TSICLIENT_TENANT_ID` will be
    removed in a future version.

You can still authenticate by passing arguments to the constructor, however,
this will be removed in a future version. Authentication works through the use of
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
    ...     applicationName="<your-app-name>">,
    ...     api_version="2020-07-31"
    ... )

Alternatively, you can use environment variables to instantiate the TSIClient.
Set the following variables (commands for Mac/Linux):

.. code-block:: console

    $ export TSICLIENT_APPLICATION_NAME=<your-app-name>
    $ export TSICLIENT_ENVIRONMENT_NAME=<your-tsi-env-name>
    $ export TSICLIENT_CLIENT_ID=<your-client-id>
    $ export TSICLIENT_CLIENT_SECRET=<your-client-secret>
    $ export TSICLIENT_TENANT_ID=<your-tenant-id>
    $ export TSI_API_VERSION="2020-07-31"

Now you can instantiate the TSIClient without passing any arguments. Be aware
that the constructor arguments take precedence over the environment variables. Specifying the
TSI api version is optional (defaults to '2020-07-31'). Allowed values are '2018-11-01-preview' and '2020-07-31'.

.. code-block:: python

    >>> from TSIClient import TSIClient as tsi
    >>> client = tsi.TSIClient()
