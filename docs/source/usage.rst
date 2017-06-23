.. _usage:

Usage
=====

``pymapd`` follows the python DB API 2.0, so this may already be familiar to you.

.. note::

   This assumes you have a MapD server running on ``localhost:9091`` with the
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


A cursor can be created with :meth:`Connection.cursor`

.. code-block:: python

   >>> c = con.cursor()
   >>> c
   <pymapd.cursor.Cursor at 0x110fe6438>

Querying
--------

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
