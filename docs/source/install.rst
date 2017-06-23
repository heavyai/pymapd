.. _install:

Install
=======

This describes how to install the python package. To setup a MapD server, see
`here`_. Currently, the library can be installed from source. In the future, it
will be available on PyPI and conda-forge.

.. code-block:: console

   git clone https://github.com/mapd/pymapd
   cd pymapd

Install the dependencies (activate your conda environment first).

.. code-block:: console

   conda install -c conda-forge six thrift

If using python2, you'll also need the ``typing`` module

.. code-block:: console

   conda install typing

Finally, install the package itself

.. code-block:: console

   pip install .

This actually installs two packages, ``pymapd``, the pythonic interface to MapD,
and ``mapd``, the Apache thrift bindings for MapD.

.. _here: https://github.com/mapd/mapd-core#mapd-core
