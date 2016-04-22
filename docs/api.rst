
API documentation
=================

.. autofunction:: dataset.connect

Database
--------

.. autoclass:: dataset.Database
   :members: tables, get_table, create_table, load_table, query, begin, commit, rollback
   :special-members:


Table
-----

.. autoclass:: dataset.Table
   :members: columns, find, find_one, all, count, distinct, insert, insert_ignore, insert_many, update, upsert, delete, create_column, drop_column, create_index, drop
   :special-members: __len__, __iter__


Data Export
-----------

.. autofunction:: dataset.freeze
