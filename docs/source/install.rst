.. _install:

Install
=======

This describes how to install the python package. To setup an OmniSci (formerly MapD) server, see
`here`_.

``pymapd`` can be installed with conda using `conda-forge`_ or pip.

.. code-block:: console

   # conda
   conda install -c conda-forge pymapd

   # pip
   pip install pymapd

This actually installs two packages, ``pymapd``, the pythonic interface to OmniSci,
and ``mapd``, the Apache thrift bindings for OmniSci.

There are several optional dependencies that may be useful. To return results sets
into GPU memory as a GpuDataFrame, you'll need `cudf`_. To return results into CPU
shared memory, using the `Apache Arrow`_ memory layout, you'll need `pyarrow`_ and
its dependencies.

.. _here: https://github.com/omnisci/mapd-core#mapd-core
.. _conda-forge: http://conda-forge.github.io/
.. _cudf: https://github.com/rapidsai/cudf
.. _pyarrow: https://arrow.apache.org/docs/python/
.. _Apache Arrow: http://arrow.apache.org/
