
Quickstart
==========


Hi, welcome to the five-minute quick-start tutorial.

Connecting to a database
------------------------

At first you need to import the dataset package :) ::

   import dataset

To connect to a database you need to identify it by its `URL <http://docs.sqlalchemy.org/en/latest/core/engines.html#engine-creation-api>`_, which basically is a string of the form ``"dialect://user:password@host/dbname"``. Here are a few common examples::

   # connecting to a SQLite database
   db = dataset.connect('sqlite:///mydatabase.db')

   # connecting to a MySQL database with user and password
   db = dataset.connect('mysql://user:password@localhost/mydatabase')

   # connecting to a PostgreSQL database
   db = dataset.connect('postgresql://scott:tiger@localhost:5432/mydatabase')


Storing data
------------

To store some data you need to get a reference to a table. You don't need to worry about whether the table already exists or not, since dataset will create it automatically::

   # get a reference to the table 'person'
   table = db['person']

Now storing data in a table is a matter of a single function call. Just pass a `dict`_ to *insert*. Note that you don't need to create the columns *name* and *age* – dataset will do this automatically::

   # Insert a new record.
   table.insert(dict(name='John Doe', age=46))

   # dataset will create "missing" columns any time you insert a dict with an unknown key
   table.insert(dict(name='Jane Doe', age=37, gender='female'))

   # If you need to insert many items at once, you can speed up things by using insert_many:
   table.insert_many(list_of_persons)

.. _dict: http://docs.python.org/2/library/stdtypes.html#dict

Updating existing entries is easy, too::

   table.update(dict(name='John Doe', age=47), ['name'])

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

Querying data
-------------

Querying data is easy. Dataset returns an iteratable result object::

   result = db.query('SELECT ...')
   for row in result:
      print row

Freezing your data
------------------



