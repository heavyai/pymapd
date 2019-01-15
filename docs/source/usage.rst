.. _usage:

.. currentmodule:: pymapd

Usage
=====

``pymapd`` follows the python DB API 2.0, so this may already be familiar to you.

.. note::

   This assumes you have an OmniSci server running on ``localhost:9091`` with the
   default logins and databases, and have loaded the example "flights_2008_10k"
   dataset.

Connecting
----------

Create a :class:`Connection` with

.. code-block:: python

   >>> from pymapd import connect
   >>> con = connect(user="mapd", password="HyperInteractive", host="localhost",
   ...               dbname="mapd")
   >>> con
   Connection(mapd://mapd:***@localhost:9091/mapd?protocol=binary)

or by passing in a connection string

.. code-block:: python

   >>> uri = "mapd://mapd:HyperInteractive@localhost:9091/mapd?protocol=binary"
   >>> con = connect(uri=uri)
   Connection(mapd://mapd:***@localhost:9091/mapd?protocol=binary)

See the `SQLAlchemy`_ documentation on what makes up a connection string. The
components are::

   dialect+driver://username:password@host:port/database

For ``pymapd``, the ``dialect+driver`` will always be ``mapd``, and we look for
a ``protocol`` argument in the optional query parameters (everything following
the ``?`` after ``database``).

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
third method, using Thrift to serialize and deserialize the data, will slower
than the GPU or CPU shared memory methods. The shared memory methods require
that your OmniSci database is running on the same machine.

GPU Select
^^^^^^^^^^

Use :meth:`Connection.select_ipc_gpu` to select data into a ``GpuDataFrame``,
provided by `cudf`_

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

CPU Shared Memory Select
^^^^^^^^^^^^^^^^^^^^^^^^

Use :meth:`Connection.select_ipc` to select data into a pandas ``DataFrame``
using CPU shared memory to avoid unnecessary intermediate copies.

.. code-block:: python

   >>> df = con.select_ipc(query)
   >>> df.head()
     depdelay arrdelay
   0       -2      -13
   1       -1      -13
   2       -3        1
   3        4       -3
   4       12        7

Cursors
-------

A cursor can be created with :meth:`Connection.cursor`

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
method available based on the type of ``data`` and whether or not ``pyarrow`` is
installed.

* lists of tuples are always loaded with :meth:`Connection.load_table_rowwise`
* If ``pyarrow`` is installed, a ``pandas.DataFrame`` or ``pyarrow.Table`` will
  be loaded using :meth:`Connection.load_table_arrow`
* If ``pyarrow`` is not installed, a ``pandas.DataFrame`` will be loaded using
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

.. _SQLAlchemy: http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
.. _cudf: http://cudf.readthedocs.io/en/latest/
.. _Apache Arrow: http://arrow.apache.org/
