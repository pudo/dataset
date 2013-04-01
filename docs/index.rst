.. dataset documentation master file, created by
   sphinx-quickstart on Mon Apr  1 18:41:21 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to dataset's documentation!
===================================

Simple API::

    import dataset
    # open a sqlite database (or create one if it doesn't exist yet)
    db = dataset.connect('sqlite:///factbook.db')

    # get a wrapper for the table 'population'
    # this will create the table if it doesn't exist yet
    table = db['population']

    # insert a new row (and also create the columns if they don't exist yet)
    table.insert(dict(country='China', year=2012, population=1354040000))
    table.insert(dict(country='India', year=2011, population=1210193422))

    # you can easily add new columns at any time
    table.insert(dict(country='United States', year=2013, population=315591999, source='http://www.census.gov'))

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


Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

