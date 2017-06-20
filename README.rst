======
pymapd
======

A python `DB API`_ compliant interface for `mapd`_.


Setup
-----

Assuming you've installed `mapd-core`_, and the additional dependencies,
the additional steps are currently

1. Generate code with thrift

```
thrift -gen py mapd.thrift
```

2. Fix the bug in the generated code

```
mv gen-py/mapd/ttypes.py gen-py/mapd/ttypes-backup.py
python SampleCode/fix_recursive_struct.py gen-py/mapd/ttypes-backup.py gen-py/mapd/ttypes.py
```

Eventually this, and placing any files in site-packages rather than patching sys.path,
will be done as part of the install step.

.. _DB API: https://www.python.org/dev/peps/pep-0249/
.. _mapd: https://www.mapd.com/
.. _mapd-core: https://github.com/mapd-core/
