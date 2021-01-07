======
pymapd
======

.. image:: https://readthedocs.org/projects/pymapd/badge/?version=latest
   :target: http://pymapd.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://jenkins-os.mapd.com/buildStatus/icon?job=pymapd-tests
   :target: https://jenkins-os.mapd.com/job/pymapd-tests/
   :alt: Jenkins Build Status

A python `DB API`_ compliant interface for `OmniSci`_ (formerly MapD). See the
`documentation`_ for more.

Quick Install (CPU)
-------------------

Packages are available on conda-forge and PyPI::

   conda install -c conda-forge pymapd

   pip install pymapd

Quick Install (GPU)
-------------------

We recommend creating a fresh conda 3.7 or 3.8 environment when installing
pymapd with GPU capabilities.

To install pymapd and cudf for GPU Dataframe support (conda-only)::

   conda create -n omnisci-gpu -c rapidsai -c nvidia -c conda-forge \
    -c defaults cudf=0.15 python=3.7 cudatoolkit=10.2 pymapd

.. _DB API: https://www.python.org/dev/peps/pep-0249/
.. _OmniSci: https://www.omnisci.com/
.. _documentation: http://pymapd.readthedocs.io/en/latest/?badge=latest
