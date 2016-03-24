
Quickstart
==========


Hi, welcome to the twelve-minute quick-start tutorial.

Connecting to a database
------------------------

At first you need to import the dataset package :) ::

   import dataset

To connect to a database you need to identify it by its `URL <http://docs.sqlalchemy.org/en/latest/core/engines.html#engine-creation-api>`_, which basically is a string of the form ``"dialect://user:password@host/dbname"``. Here are a few examples for different database backends::

   # connecting to a SQLite database
   db = dataset.connect('sqlite:///mydatabase.db')

   # connecting to a MySQL database with user and password
   db = dataset.connect('mysql://user:password@localhost/mydatabase')

   # connecting to a PostgreSQL database
   db = dataset.connect('postgresql://scott:tiger@localhost:5432/mydatabase')

It is also possible to define the `URL` as an environment variable called `DATABASE_URL`
so you can initialize database connection without explicitly passing an `URL`::

   db = dataset.connect()

Depending on which database you're using, you may also have to install
the database bindings to support that database. SQLite is included in
the Python core, but PostgreSQL requires ``psycopg2`` to be installed. 
MySQL can be enabled by installing the ``mysql-db`` drivers. 


Storing data
------------

To store some data you need to get a reference to a table. You don't need
to worry about whether the table already exists or not, since dataset
will create it automatically::

   # get a reference to the table 'user'
   table = db['user']

Now storing data in a table is a matter of a single function call. Just
pass a `dict`_ to *insert*. Note that you don't need to create the columns
*name* and *age* – dataset will do this automatically::

   # Insert a new record.
   table.insert(dict(name='John Doe', age=46, country='China'))

   # dataset will create "missing" columns any time you insert a dict with an unknown key
   table.insert(dict(name='Jane Doe', age=37, country='France', gender='female'))

.. _dict: http://docs.python.org/2/library/stdtypes.html#dict

Updating existing entries is easy, too::

   table.update(dict(name='John Doe', age=47), ['name'])

The list of filter columns given as the second argument filter using the
values in the first column. If you don't want to update over a
particular value, just use the auto-generated ``id`` column.

Using Transactions
------------------

You can group a set of database updates in a transaction. In that case, all updates
are committed at once or, in case of exception, all of them are reverted. Transactions
are supported through a context manager, so they can be used through a ``with``
statement::

    with dataset.connect() as tx:
        tx['user'].insert(dict(name='John Doe', age=46, country='China'))

You can get same functionality by invoking the methods :py:meth:`begin() <dataset.Table.begin>`,
:py:meth:`commit() <dataset.Table.commit>` and :py:meth:`rollback() <dataset.Table.rollback>`
explicitly::

    db = dataset.connect()
    db.begin()
    try:
        db['user'].insert(dict(name='John Doe', age=46, country='China'))
        db.commit()
    except:
        db.rollback()

Nested transactions are supported too::

    db = dataset.connect()
    with db as tx1:
        tx1['user'].insert(dict(name='John Doe', age=46, country='China'))
        with db as tx2:
            tx2['user'].insert(dict(name='Jane Doe', age=37, country='France', gender='female'))



Inspecting databases and tables
-------------------------------

When dealing with unknown databases we might want to check their structure
first. To start exploring, let's find out what tables are stored in the
database:

   >>> print(db.tables)
   [u'user']

Now, let's list all columns available in the table ``user``:

   >>> print(db['user'].columns)
   [u'id', u'country', u'age', u'name', u'gender'] 

Using ``len()`` we can get the total number of rows in a table:

   >>> print(len(db['user']))
   2

Reading data from tables
------------------------

Now let's get some real data out of the table::

   users = db['user'].all()

If we simply want to iterate over all rows in a table, we can omit :py:meth:`all() <dataset.Table.all>`::

   for user in db['user']:
      print(user['age'])

We can search for specific entries using :py:meth:`find() <dataset.Table.find>` and
:py:meth:`find_one() <dataset.Table.find_one>`::

   # All users from China
   chinese_users = table.find(country='China')

   # Get a specific user
   john = table.find_one(name='John Doe')

   # Find by comparison
   elderly_users = table.find(table.table.columns.age >= 70)

Using  :py:meth:`distinct() <dataset.Table.distinct>` we can grab a set of rows
with unique values in one or more columns::

   # Get one user per country
   db['user'].distinct('country')

Finally, you can use the ``row_type`` parameter to choose the data type in which
results will be returned::

    import dataset
    from stuf import stuf

    db = dataset.connect('sqlite:///mydatabase.db', row_type=stuf)

Now contents will be returned in ``stuf`` objects (basically, ``dict``
objects whose elements can be acessed as attributes (``item.name``) as well as 
by index (``item['name']``).

Running custom SQL queries
--------------------------

Of course the main reason you're using a database is that you want to
use the full power of SQL queries. Here's how you run them with ``dataset``::

   result = db.query('SELECT country, COUNT(*) c FROM user GROUP BY country')
   for row in result:
      print(row['country'], row['c'])

The :py:meth:`query() <dataset.Table.query>` method can also be used to 
access the underlying `SQLAlchemy core API <http://docs.sqlalchemy.org/en/latest/orm/query.html#the-query-object>`_, which allows for the
programmatic construction of more complex queries::

   table = db['user'].table
   statement = table.select(table.c.name.like('%John%'))
   result = db.query(statement) 


Exporting data
--------------

While playing around with our database in Python is a nice thing, they are 
sometimes just a processing stage until we go on to use it in another
place, say in an interactive web application. To make this seamless,
``dataset`` supports serializing rows of data into static JSON and CSV files
such using the :py:meth:`freeze() <dataset.freeze>` function::

   # export all users into a single JSON
   result = db['users'].all()
   dataset.freeze(result, format='json', filename='users.json')

You can create one file per row by setting ``mode`` to "item"::

   # export one JSON file per user
   dataset.freeze(result, format='json', filename='users/{{ id }}.json', mode='item')

Since this is a common operation we made it available via command line
utility ``datafreeze``. Read more about the :doc:`freezefile markup <freezefile>`.

.. code-block:: bash

   $ datafreeze freezefile.yaml
