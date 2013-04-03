
Quickstart
==========


Hi, welcome to the twelve-minute quick-start tutorial.

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

Inspecting databases and tables
-------------------------------

When dealing with unknown databases we might want to check its structure first. To begin with, let's find out what tables are stored in the database:

   >>> print db.tables
   set([u'user', u'action'])

Now, let's list all columns available in the table ``user``:

   >>> print db['user'].columns
   set([u'id', u'name', u'email', u'pwd', u'country'])

Using ``len()`` we can get the total number of rows in a table:

   >>> print len(db['user'])
   187

Reading data from tables
------------------------

Now let's get some real data out of the table::

   users = db['user'].all()

If we simply want to iterate over all rows in a table, we can ommit :py:meth:`all() <dataset.Table.all>`::

   for user in db['user']:
      print user['email']

We can search for specific entries using :py:meth:`find() <dataset.Table.find>` and :py:meth:`find_one() <dataset.Table.find_one>`::

   # All users from China
   users = table.find(country='China')

   # Get a specific user
   john = table.find_one(name='John Doe')

Using  :py:meth:`distinct() <dataset.Table.distinct>` we can grab a set of rows with unique values in one or more columns::

   # Get one user per country
   db['user'].distinct('country')


Running custom SQL queries
--------------------------

Of course the main reason you're using a database is that you want to use the full power of SQL queries. Here's how you run them with ``dataset``::

   result = db.query('SELECT country, COUNT(*) c FROM user GROUP BY country')
   for row in result:
      print row['country'], row['c']

If you are familiar with `SQLAlchemy query expressions <http://docs.sqlalchemy.org/ru/latest/orm/query.html#the-query-object>`_ you can use them, too::

   q = session.query(MyClass).filter_by(name = 'some name')
   result = db.query(q)

Exporting data
--------------

While playing around with our database in Python is a nice thing, sometimes we want to use the data –or parts of it– elsewhere, say in an interactive web application. Therefor ``dataset`` supports serializing rows of data into static files such as JSON using the :py:meth:`freeze() <dataset.freeze>` function::

   # export all users into a single JSON
   result = db['users'].all()
   dataset.freeze(result, 'users.json')

You can create one file per row by setting ``mode`` to "item"::

   # export one JSON file per user
   dataset.freeze(result, 'users/{{ id }}.json', mode='item')


Since this is a common operation we made it available via command line utility ``datafreeze``. Read more about the `freezefile markup <https://github.com/spiegelonline/datafreeze#example-freezefileyaml>`_.

.. code-block:: bash

   $ datafreeze freezefile.yaml
