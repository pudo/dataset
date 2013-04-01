
Contents:

.. toctree::
   :maxdepth: 2



.. autofunction:: dataset.connect


Database
========

A Database is a simple wrapper around SQLAlchemy engines. Most of the time you want to use it to get instances to tables using *get_table* or the short-hand dict syntax::

   # both statements return the same table
   table = db['population']
   table = db.get_table('population')

.. autoclass:: dataset.Database
   :members: get_table, create_table, load_table, query
   :undoc-members:



Table
=====

Using the *Table* class you can easily store and retreive data from database tables.

.. autoclass:: dataset.Table
   :members:
   :undoc-members:
