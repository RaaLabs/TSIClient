Raa Labs reference
==================
This section gives additional information for people working at Raa Labs.

Publish package versions to PyPi
################################
Follow these steps to create a new version of the package:
1. Install the following python packages: ``twine``, ``wheel`` and ``setuptools``
2. Increment the version in ``setup.py``
2. Run ``python setup.py sdist bdist_wheel``
3. Run ``twine upload dist/*`` (you will be prompted for your PyPi account/password)
