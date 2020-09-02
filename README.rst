======
pymapd
======

A wrapper for the pyomnisci libarary http://github.com/omnisci/pyomnisci, maintained for backwards compatibility.

Existing scripts should be migrated to pyomnisci from pymapd, this library will
not be updated moving forward.

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
.. _pyomnisci: http://github.com/omnisci/pyomnisci
.. _OmniSci: https://www.omnisci.com/
