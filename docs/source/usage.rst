.. _usage:

.. currentmodule:: pymapd

5-Minute Quickstart
===================

``pymapd`` follows the python DB API 2.0, so experience with other Python database
clients will feel similar to pymapd.

.. note::

   This tutorial assumes you have an OmniSci server running on ``localhost:6274`` with the
   default logins and databases, and have loaded the example ``flights_2008_10k``
   dataset. This dataset can be obtained from the ``insert_sample_data`` script included
   in the OmniSci install directory.

Installing pymapd
-----------------

pymapd
******

``pymapd`` can be installed with conda using `conda-forge`_ or pip.

.. code-block:: console

   # conda
   conda install -c conda-forge pymapd

   # pip
   pip install pymapd

If you have an NVIDIA GPU in the same machine where your pymapd code will be running, you'll want to `install
cudf`_ as well to return results sets into GPU memory as a cudf GPU DataFrame:

cudf via conda
**************

.. code-block:: console

   # CUDA 9.2
   conda install -c nvidia -c rapidsai -c numba -c conda-forge -c defaults cudf

   # CUDA 10.0
   conda install -c nvidia/label/cuda10.0 -c rapidsai/label/cuda10.0 -c numba \
       -c conda-forge -c defaults cudf

cudf via PyPI/pip
*****************

.. code-block:: console

   # CUDA 9.2
   pip install cudf-cuda92

   # CUDA 10.0
   pip install cudf-cuda100

Connecting
----------

Self-Hosted Install
*******************

For self-hosted OmniSci installs, use ``protocol='binary'`` (this is the default)
to connect with OmniSci, as this will have better performance than using
``protocol='http'`` or ``protocol='https'``.

To create a :class:`Connection` using the ``connect()`` method along with ``user``,
``password``, ``host`` and ``dbname``:

.. code-block:: python

   >>> from pymapd import connect
   >>> con = connect(user="admin", password="HyperInteractive", host="localhost",
   ...               dbname="omnisci")
   >>> con
   Connection(mapd://admin:***@localhost:6274/omnisci?protocol=binary)

Alternatively, you can pass in a `SQLAlchemy`_-compliant connection string to
the ``connect()`` method:

.. code-block:: python

   >>> uri = "mapd://admin:HyperInteractive@localhost:6274/omnisci?protocol=binary"
   >>> con = connect(uri=uri)
   Connection(mapd://admin:***@localhost:6274/omnisci?protocol=binary)

OmniSci Cloud
*************

When connecting to OmniSci Cloud, the two methods are the same as above,
however you can only use ``protocol='https'``. For a step-by-step walk-through with
screenshots, please see this `blog post`_.


Querying
--------

A few options are available for getting the results of a query into your Python
process.

1. Into GPU Memory via `cudf`_ (:meth:`Connection.select_ipc_gpu`)
2. Into CPU shared memory via Apache Arrow and pandas
   (:meth:`Connection.select_ipc`)
3. Into python objects via Apache Thrift (:meth:`Connection.execute`)

The best option depends on the hardware you have available, your connection to
the database, and what you plan to do with the returned data. In general, the
third method, using Thrift to serialize and deserialize the data, will be slower
than the GPU or CPU shared memory methods. The shared memory methods require
that your OmniSci database is running on the same machine.

.. note::

   We currently support ``Timestamp(0|3|6)`` data types i.e. seconds, milliseconds,
   and microseconds granularity. Support for nanoseconds, ``Timestamp(9)`` is in
   progress.

GPU Shared Memory
*****************

Use :meth:`Connection.select_ipc_gpu` to select data into a ``GpuDataFrame``,
provided by `cudf`_. To use this method, **the Python code must be running
on the same machine as the OmniSci installation AND you must have an NVIDIA GPU
installed.**

.. code-block:: python

   >>> query = "SELECT depdelay, arrdelay FROM flights_2008_10k limit 100"
   >>> df = con.select_ipc_gpu(query)
   >>> df.head()
     depdelay arrdelay
   0       -2      -13
   1       -1      -13
   2       -3        1
   3        4       -3
   4       12        7

CPU Shared Memory
*****************

Use :meth:`Connection.select_ipc` to select data into a pandas ``DataFrame``
using CPU shared memory to avoid unnecessary intermediate copies. To use this
method, **the Python code must be running on the same machine as the OmniSci
installation.**

.. code-block:: python

   >>> df = con.select_ipc(query)
   >>> df.head()
     depdelay arrdelay
   0       -2      -13
   1       -1      -13
   2       -3        1
   3        4       -3
   4       12        7

pandas.read_sql()
*****************

With a :class:`Connection` defined, you can use ``pandass.read_sql()`` to
read your data in a pandas ``DataFrame``. This will be slower than using
:meth:`Connection.select_ipc`, but works regardless of where the Python code
is running (i.e. ``select_ipc()`` must be on the same machine as the OmniSci
install, ``pandas.read_sql()`` works everywhere):

.. code-block:: python

   >>> from pymapd import connect
   >>> import pandas as pd
   >>> con = connect(user="admin", password="HyperInteractive", host="localhost",
   ...               dbname="omnisci")
   >>> df = pd.read_sql("SELECT depdelay, arrdelay FROM flights_2008_10k limit 100", con)


Cursors
*******

After connecting to OmniSci, a cursor can be created with :meth:`Connection.cursor`:

.. code-block:: python

   >>> c = con.cursor()
   >>> c
   <pymapd.cursor.Cursor at 0x110fe6438>

Or by using a context manager:

.. code-block:: python

   >>> with con as c:
   ...     print(c)
   <pymapd.cursor.Cursor object at 0x1041f9630>

Arbitrary SQL can be executed using :meth:`Cursor.execute`.

.. code-block:: python

   >>> c.execute("SELECT depdelay, arrdelay FROM flights_2008_10k limit 100")
   <pymapd.cursor.Cursor at 0x110fe6438>

This will set the ``rowcount`` property, with the number of returned rows

   >>> c.rowcount
   100

The ``description`` attribute contains a list of ``Description`` objects, a
namedtuple with the usual attributes required by the spec. There's one entry per
returned column, and we fill the ``name``, ``type_code`` and ``null_ok`` attributes.

   >>> c.description
   [Description(name='depdelay', type_code=0, display_size=None, internal_size=None, precision=None, scale=None, null_ok=True),
    Description(name='arrdelay', type_code=0, display_size=None, internal_size=None, precision=None, scale=None, null_ok=True)]

Cursors are iterable, returning a list of tuples of values

   >>> result = list(c)
   >>> result[:5]
   [(38, 28), (0, 8), (-4, 9), (1, -1), (1, 2)]

Loading Data
------------

The fastest way to load data is :meth:`Connection.load_table_arrow`. Internally,
this will use ``pyarrow`` and the `Apache Arrow`_ format to exchange data with
the OmniSci database.

.. code-block:: python

   >>> import pyarrow as pa
   >>> import pandas as pd
   >>> df = pd.DataFrame({"A": [1, 2], "B": ['c', 'd']})
   >>> table = pa.Table.from_pandas(df)
   >>> con.load_table_arrow("table_name", table)

This accepts either a ``pyarrow.Table``, or a ``pandas.DataFrame``, which will
be converted to a ``pyarrow.Table`` before loading.

You can also load a ``pandas.DataFrame`` using :meth:`Connection.load_table`
or :meth:`Connection.load_table_columnar` methods.

.. code-block:: python

   >>> df = pd.DataFrame({"A": [1, 2], "B": ["c", "d"]})
   >>> con.load_table_columnar("table_name", df, preserve_index=False)

If you aren't using arrow or pandas you can pass list of tuples to
:meth:`Connection.load_table_rowwise`.

.. code-block:: python

   >>> data = [(1, "c"), (2, "d")]
   >>> con.load_table_rowwise("table_name", data)


The high-level :meth:`Connection.load_table` method will choose the fastest
method available based on the type of ``data``.

* lists of tuples are always loaded with :meth:`Connection.load_table_rowwise`
* A ``pandas.DataFrame`` or ``pyarrow.Table`` will be loaded using :meth:`Connection.load_table_arrow`
* If upload fails using the arrow method, a ``pandas.DataFrame`` can be loaded using
  :meth:`Connection.load_table_columnar`

Database Metadata
-----------------

Some helpful metadata are available on the ``Connection`` object.

1. Get a list of tables with :meth:`Connection.get_tables`

.. code-block:: python

   >>> con.get_tables()
   ['flights_2008_10k', 'stocks']

2. Get column information for a table with :meth:`Connection.get_table_details`

   >>> con.get_table_details('stocks')
   [ColumnDetails(name='date_', type='STR', nullable=True, precision=0,
                  scale=0, comp_param=32),
    ColumnDetails(name='trans', type='STR', nullable=True, precision=0,
                  scale=0, comp_param=32),
    ...

Runtime User-Defined Functions
------------------------------

Connection instance is callable, it can be used as a decorator to
Python functions to define these as Runtime UDFs:

.. code-block:: python

   >>> @con('int32(int32, int32)')
   ... def totaldelay(dep, arr):
   ...     return dep + arr
   ...
   >>> query = ("SELECT depdelay, arrdelay, totaldelay(depdelay, arrdelay)"
   ...          " FROM flights_2008_10k limit 100")
   >>> df = con.select_ipc(query)
   >>> df.head()
      depdelay  arrdelay  EXPR$2
   0         8       -14      -6
   1        19         2      21
   2         8        14      22
   3        -4        -6     -10
   4        34        34      68

.. note::

   Runtime UDFs can be defined if the OmniSci server has enabled its
   support (see ``--enable-runtime-udf`` option of ``omnisci_server``)
   and `rbc`_ package is installed. This is still experimental functionality, and
   currently it does not work on the Windows operating system.

.. _SQLAlchemy: http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
.. _cudf: https://rapidsai.github.io/projects/cudf/en/latest/
.. _Apache Arrow: http://arrow.apache.org/
.. _conda-forge: http://conda-forge.github.io/
.. _install cudf: https://github.com/rapidsai/cudf#installation
.. _blog post: https://www.omnisci.com/blog/using-pymapd-to-load-data-to-omnisci-cloud
.. _rbc : https://github.com/xnd-project/rbc
