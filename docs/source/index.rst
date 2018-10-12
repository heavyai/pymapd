.. pymapd documentation master file, created by
   sphinx-quickstart on Fri Jun 23 12:29:54 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pymapd
======

The pymapd client interface provides a python DB API 2.0-compliant `OmniSci`_
interface (formerly MapD). In addition, it provides methods to get results in
the `Apache Arrow`_ -based GDF format for efficient data interchange.

.. code-block:: python

   >>> from pymapd import connect
   >>> con = connect(user="mapd", password="HyperInteractive", host="localhost",
   ...               dbname="mapd")
   >>> query = "SELECT depdelay, arrdelay FROM flights_2008_10k limit 100"
   >>> df = con.select_ipc_gpu("SELECT depdelay, arrdelay "
   ...                         "FROM   flights_2008_10k "
   ...                         "LIMIT  100")
   >>> df.head()
     depdelay arrdelay
   0       -2      -13
   1       -1      -13
   2       -3        1
   3        4       -3
   4       12        7

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   install
   usage
   api
   contributing


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. _DB-API-2.0: https://www.python.org/dev/peps/pep-0249/
.. _OmniSci: https://www.omnisci.com/
.. _Apache Arrow: http://arrow.apache.org/
