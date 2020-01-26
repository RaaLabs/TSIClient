Developer reference
===================
If you want to contribute to the development of TSIClient,
this section helps you to get you started.

Setup
#####
Fork the repository, and install the dependencies. We recommend using
a virtual environment when developing (eg ``virtualenv`` or ``pipenv``), but that is up to you.
Below are the commands for Mac/Linux.

.. code-block:: console

    $ git clone https://github.com/RaaLabs/TSIClient
    $ virtualenv venv
    $ source venv/bin/activate
    $ pip install -r requirements

Testing
#######
We use ``pytest`` for testing and you can run unit tests with:

.. code-block:: console

    $ python -m pytest -v

Documentation
#############
We use ``sphinx`` and Read the Docs for documentation,
with separate source and build directory. Please refer to the
documentation to get started: https://docs.readthedocs.io/en/stable/intro/getting-started-with-sphinx.html.
You can generate automated documentation from docstrings locally by running the
following commands (from the ``docs`` directory):

.. code-block:: console

    $ sphinx-apidoc ../TSIClient -o docs/build -f -e -P

And build the documentation with:

.. code-block:: console

    $ sphinx-build source build

Now you can open the html documentation by opening ``docs/build/index.html``.
