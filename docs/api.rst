
API documentation
=================

.. autofunction:: dataset.connect

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

Model
-----

.. autoclass:: dataset.models.Model
   :members: __init__, save, delete, find, find_one

Data Export
-----------

.. autofunction:: dataset.freeze
