.. dataset documentation master file, created by
   sphinx-quickstart on Mon Apr  1 18:41:21 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to dataset's documentation!
===================================

Contents:

.. toctree::
   :maxdepth: 2



Quick-start
===========

At first you need to import the package :). To connect to a database you need to identify it using
what is called an engine url. Here are a few examples::

   import dataset

   # connecting to a SQLite database
   db = dataset.connect('sqlite:///factbook.db')

   # connecting to a MySQL database
   db = dataset.connect('mysql:///')


Storing data
------------
Storing data in a table is as simple. **dataset** will automatically create the columns, if they don't exist yet::

   table.insert(dict(country='China', year=2012, population=1354040000))


Reading data from tables
------------------------

Checking::

   table = db['population']

   # Let's grab a list of all items/rows/entries in the table:
   table.all()

   table.distinct()

Searching for specific entries::

   # Returns the first item where the column country equals 'China'
   table.find_one(country='China')

   # Returns all items
   table.find(country='China')




You can add additional columns at any time::

   table.insert(dict(country='US', year=2013, population=315591999, source='http://www.census.gov'))

Updating existing entries::

   table.update(dict(country='China', year=2012, population=1354040001), ['country', 'year'])






Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

