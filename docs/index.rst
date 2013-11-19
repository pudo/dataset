.. dataset documentation master file, created by
   sphinx-quickstart on Mon Apr  1 18:41:21 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

dataset: databases for lazy people
==================================

.. toctree::
   :hidden:


Although managing data in relational database has plenty of benefits, they're rarely used in day-to-day work with small to medium scale datasets. But why is that? Why do we see an awful lot of data stored in static files in CSV or JSON format, even though they are hard
to query and update incrementally?

The answer is that **programmers are lazy**, and thus they tend to prefer the easiest solution they find. And in **Python**, a database isn't the simplest solution for storing a bunch of structured data. This is what **dataset** is going to change!

**dataset** provides two key functions that make using SQL databases in
Python a breeze: 

* A simple abstraction layer removes most direct SQL statements without
  the necessity for a full ORM model - essentially, databases can be
  used like a JSON file or NoSQL store.

* Database contents can be exported (*frozen*) using a :doc:`sophisticated
  plain file generator <freezefile>` with JSON and CSV support. Exports can be configured
  to include metadata and dynamic file names depending on the exported
  data. The exporter can also be used as a command-line tool, ``datafreeze``.

A simple data loading script using **dataset** might look like this:

::

   import dataset

   db = dataset.connect('sqlite:///:memory:')

   table = db['sometable']
   table.insert(dict(name='John Doe', age=37))
   table.insert(dict(name='Jane Doe', age=34, gender='female'))

   john = table.find_one(name='John Doe')


Here is `similar code, without dataset <https://gist.github.com/gka/5296492>`_.


Features
--------

* **Automatic schema**: If a table or column is written that does not
  exist in the database, it will be created automatically.
* **Upserts**: Records are either created or updated, depending on
  whether an existing version can be found.
* **Query helpers** for simple queries such as :py:meth:`all <dataset.Table.all>` rows in a table or
  all :py:meth:`distinct <dataset.Table.distinct>` values across a set of columns.
* **Compatibility**: Being built on top of `SQLAlchemy <http://www.sqlalchemy.org/>`_, ``dataset`` works with all major databases, such as SQLite, PostgreSQL and MySQL.
* **Scripted exports**: Data can be exported based on a scripted
  configuration, making the process easy and replicable.

Contents
--------

.. toctree::
   :maxdepth: 2

   install
   quickstart
   freezefile
   api

Contributors
------------

``dataset`` is written and maintained by `Friedrich Lindenberg <https://github.com/pudo>`_ and `Gregor Aisch <https://github.com/gka>`_. Its code is largely based on the preceding libraries `sqlaload <https://github.com/okfn/sqlaload>`_ and `datafreeze <https://github.com/spiegelonline/datafreeze>`_. And of course, we're standing on the `shoulders of giants <http://www.sqlalchemy.org/>`_.

Our cute little `naked mole rat <http://www.youtube.com/watch?feature=player_detailpage&v=A5DcOEzW1wA#t=14s>`_ was drawn by `Johannes Koch <http://chechuchape.com/>`_.
