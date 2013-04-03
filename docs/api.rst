
API documentation
=================

.. autofunction:: dataset.connect

Database
--------

.. autoclass:: dataset.Database
   :members: get_table, create_table, load_table, query, tables
   :special-members:


Table
-----

.. autoclass:: dataset.Table
   :members: columns, drop, insert, update, upsert, find, find_one, distinct, create_column, create_index, all
   :special-members: __len__, __iter__
