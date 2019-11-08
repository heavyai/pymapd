.. _releasenotes:

Release Notes
=============

The release notes for pymapd are managed on the GitHub repository in the `Releases tab`_. Since pymapd
releases try to track new features in the main OmniSci Core project, it's highly recommended that you check
the Releases tab any time you install a new version of pymapd or upgrade OmniSci so that you understand any breaking
changes that may have been made during a new pymapd release.

Some notable breaking changes include:

.. table::
   :widths: auto
   :align: left

   =======    ===============
   Release    Breaking Change
   =======    ===============
   `0.17`_    Added preliminary support for Runtime User-Defined Functions
   `0.15`_    Support for binary TLS Thrift connections
   `0.14`_    Updated Thrift bindings to 4.8
   `0.13`_    Changed context manager to return ``self``, not next level down
   `0.12`_    Updated Thrift to 4.6.1 bindings
   `0.11`_    Dropped Python 3.5 support
   `0.11`_    Modified ``load_table_columnar`` to support OmniSci 4.6 backend Dates change
   `0.10`_    ``Int8`` when using Arrow for data upload no longer mutates input DataFrame
   `0.10`_    ``load_table*`` methods set string columns to ``TEXT ENCODING DICT(32)``
   `0.9`_     Removed ability to specify ``columnar`` keyword on ``Cursor``
   `0.9`_     Lower bounds for pandas, numpy, sqlalchemy and pytest increased
   `0.8`_     Default ports changed in connect statement from 9092 to 6274
   `0.8`_     Python 2 support dropped
   `0.7`_     Support for Python 3.4 dropped, support for Python 3.7 added
   `0.7`_     First release supporting cudf (removing option to use pygdf)
   `0.6`_     NumPy, pyarrow and pandas now hard dependencies instead of optional
   =======    ===============



.. _Releases tab: https://github.com/omnisci/pymapd/releases
.. _0.6: https://github.com/omnisci/pymapd/releases/tag/v0.6.0
.. _0.7: https://github.com/omnisci/pymapd/releases/tag/v0.7.0
.. _0.8: https://github.com/omnisci/pymapd/releases/tag/v0.8.0
.. _0.9: https://github.com/omnisci/pymapd/releases/tag/v0.9.0
.. _0.10: https://github.com/omnisci/pymapd/releases/tag/v0.10.0
.. _0.11: https://github.com/omnisci/pymapd/releases/tag/v0.11.0
.. _0.12: https://github.com/omnisci/pymapd/releases/tag/v0.12.0
.. _0.13: https://github.com/omnisci/pymapd/releases/tag/v0.13.0
.. _0.14: https://github.com/omnisci/pymapd/releases/tag/v0.14.0
.. _0.15: https://github.com/omnisci/pymapd/releases/tag/v0.15.0
.. _0.17: https://github.com/omnisci/pymapd/releases/tag/v0.17.0
