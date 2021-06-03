
API documentation
=================

Connecting
----------

.. autofunction:: dataset.connect

Notes
-----

* **dataset** uses SQLAlchemy connection pooling when connecting to the
  database. There is no way of explicitly clearing or shutting down the
  connections, other than having the dataset instance garbage collected.

Database
--------

.. autoclass:: dataset.Database
   :members: tables, get_table, create_table, load_table, query, begin, commit, rollback
   :special-members:


Table
-----

.. autoclass:: dataset.Table
   :members: columns, find, find_one, all, count, distinct, insert, insert_ignore, insert_many, update, update_many, upsert, upsert_many, delete, create_column, create_column_by_example, drop_column, create_index, drop, has_column, has_index
   :special-members: __len__, __iter__


Data Export
-----------


  **Note:** Data exporting has been extracted into a stand-alone package, datafreeze. See the relevant repository here_.

.. _here: https://github.com/pudo/datafreeze

|

.. autofunction:: datafreeze.freeze
