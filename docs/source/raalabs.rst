Raa Labs reference
==================
This section gives additional information for people working at Raa Labs.

Publish package versions to PyPi
################################
Follow these steps to create a new version of the package:

1. Install the following python packages: ``twine``, ``wheel`` and ``setuptools``
2. Increment the version in ``setup.py``

3. Run the ``setup.py``:

.. code-block:: console

    $ python setup.py sdist bdist_wheel

This will create a ``build``, ``dist`` and ``TSIClient.egg-info`` folder.

4. Upload the package to PyPi (you will be prompted for your PyPi account/password):

.. code-block:: console

    $ python setup.py sdist bdist_wheel
