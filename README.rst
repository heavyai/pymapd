======
pymapd
======

.. image:: https://travis-ci.org/omnisci/pymapd.svg?branch=master
   :target: https://travis-ci.org/omnisci/pymapd


.. image:: https://readthedocs.org/projects/pymapd/badge/?version=latest
   :target: http://pymapd.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

A python `DB API`_ compliant interface for `OmniSci`_ (formerly MapD). See the
`documentation`_ for more.

Quick Install
-------------

Packages are available on conda-forge and PyPI::

   conda install -c conda-forge pymapd

   pip install pymapd

To install cudf for GPU Dataframe support (conda-only)::

   conda install -c conda-forge -c nvidia -c rapidsai -c numba -c defaults pymapd cudf python=3.6


.. _DB API: https://www.python.org/dev/peps/pep-0249/
.. _OmniSci: https://www.omnisci.com/
.. _documentation: http://pymapd.readthedocs.io/en/latest/?badge=latest
