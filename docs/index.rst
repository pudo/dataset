.. dataset documentation master file, created by
   sphinx-quickstart on Mon Apr  1 18:41:21 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

dataset: databases for lazy people
==================================

.. toctree::
   :hidden:


Although managing data in relational database has plenty of benefits, we find them rarely being used in the typical day-to-day work with small to medium scale datasets. But why is that? Why do we see an awful lot of data stored in static files in CSV or JSON format?

Because **programmers are lazy**, and thus they tend to prefer the easiest solution they find. And managing data in a databases simply wasn't the simplest solution to store a bunch of structured data. This is where ``dataset`` steps in!

Dataset is here to **take the pain out of databases**. It makes reading and writing data in databases as simple as reading and writing JSON files.

::

   import dataset

   db = dataset.connect('sqlite:///database.db')
   db['sometable'].insert(dict(name='John Doe', age=37))
   db['sometable'].insert(dict(name='Jane Doe', age=34, gender='female'))


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

Contents
--------

.. toctree::
   :maxdepth: 2

   quickstart
   api

