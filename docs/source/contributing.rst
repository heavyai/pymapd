.. _contributing:

Development Setup
=================

TODO

Thrift Binding
==============

When the upstream `mapd-core`_ project updates its Thrift definition, we have to
regenerate the bindings we ship with ``pymapd``. From the root of the ``pymapd``
repository, run

.. code-block:: shell

   python scripts/generate_accelerated_bindings.py </path/to/mapd-core>/mapd.thrift


The requires that Thrift is installed and on your PATH. Running it will update
two files, ``mapd/MapD.py`` and ``mapd/ttypes.py``, which can be committed to
the repository.


.. _mapd-core: https://github.com/omnisci/mapd-core
