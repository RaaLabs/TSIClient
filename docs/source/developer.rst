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

    $ git clone https://github.com/<your-github-name>/TSIClient
    $ virtualenv venv
    $ source venv/bin/activate
    $ pip install -r requirements

For Windows, use 

.. code-block:: console

    $ venv/scripts/activate

to activate the virtual environment.


Testing
#######
We use ``pytest`` for testing and ``requests-mock`` for mocking api calls
to the TSI APIs within unittests.

You can run tests with:

.. code-block:: console

    $ python -m pytest -v

Docstrings & Documentation
##########################
Docstrings are written according the Google style for Python docstrings.
See an example here: https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html.

We use ``sphinx`` and Read the Docs for documentation,
with separate source and build directory. Please refer to the
documentation to get started: https://docs.readthedocs.io/en/stable/intro/getting-started-with-sphinx.html.
You can generate automated documentation from docstrings locally by running the
following commands (from the ``docs`` directory):

.. code-block:: console

    $ sphinx-apidoc -f -e -o ./source/_autogen/ ../TSIClient --ext-autodoc --private

And build the documentation with:

.. code-block:: console

    $ sphinx-build source build

Now you can open the html documentation by opening ``docs/build/index.html``.


Pull requests
#############
Please write tests and docstrings for functionality that you add
before submitting a pull request. Make sure to update/generate the
.rst files by running the ``sphinx-apidoc`` command.
