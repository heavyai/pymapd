.. _faq:

FAQ and Known Limitations
=========================

This page contains information that doesn't fit into other pages or is
important enough to be called out separately. If you have a question or tidbit
of information that you feel should be included here, please create an `issue`_
and/or `pull request`_ to get it added to this page.

.. note::
    While we strive to keep this page updated, bugfixes and new features
    are being added regularly. If information on this page conflicts with
    your experience, please open an `issue`_ or drop by our `Community forum`_
    to get clarification.


FAQ
***

:Q: Why do ``select_ipc()`` and ``select_ipc_gpu()`` give me errors, but ``execute()``
    works fine?

:A: Both ``select_ipc()`` and ``select_ipc_gpu()`` require running the pymapd code
    on the same machine where OmniSci is running. This also implies that these two
    methods will not work on Windows machines, just Linux (CPU and GPU) and OSX (CPU-only).

..

:Q: Why do geospatial data get uploaded as ``TEXT ENCODED DICT(32)``?

:A: When using ``load_table`` with ``create=True`` or ``create='infer'``, data
    where type cannot be easily inferred will default to ``TEXT ENCODED DICT(32)``.
    To solve this issue, create the table definition before loading the data.



Helpful Hints
*************

* Convert your timestamps to UTC
    OmniSci stores timestamps as UTC. When loading data to OmniSci, plain Python
    ``datetime`` objects are assumed to be UTC. If the ``datetime`` object has
    localization, only ``datetime64[ns, UTC]`` is supported.

* When loading data, hand-create table schema if performance is critical
    While the ``load_table()`` does provide a keyword argument ``create`` to
    auto-create the table before attempting to load to OmniSci, this functionality
    is for *convenience purposes only*. The user is in a much better position
    to know the exact data types of the input data than the heuristics used by pymapd.

    Additionally, pymapd does not attempt to use the smallest possible column
    width to represent your data. For example, significant reductions in disk
    storage and a larger amount of 'hot data' can be realized if your data fits
    in a ``TINYINT`` column vs storing it as an ``INTEGER``.

Known Limitations
*****************

* OmniSci ``BIGINT`` is 64-bit
    Be careful using pymapd on 32-bit systems, as we do not check for integer
    overflow when returning a query.

* ``DECIMAL`` types returned as Python ``float``
    OmniSci stores and performs ``DECIMAL`` calculations within the
    database at the column-definition level of precision. However, the results
    are currently returned back to Python as float. We are evaluating how to
    change this behavior, so that exact decimal representations is consistent on
    the server and in Python.


.. _issue: https://github.com/omnisci/pymapd/issues
.. _pull request: https://github.com/omnisci/pymapd/issues
.. _Community forum: https://community.omnisci.com/forum
