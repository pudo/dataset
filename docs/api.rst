
API documentation
=================

.. autofunction:: dataset.connect

.. autofunction:: dataset.freeze

Database
--------

.. autoclass:: dataset.Database
   :members: tables, get_table, create_table, load_table, query
   :special-members:


Table
-----

.. autoclass:: dataset.Table
   :members: columns, find, find_one, all, distinct, insert, insert_many, update, upsert,   create_column, create_index, drop
   :special-members: __len__, __iter__
