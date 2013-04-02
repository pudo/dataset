.. dataset documentation master file, created by
   sphinx-quickstart on Mon Apr  1 18:41:21 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

dataset: databases for humans
=============================

Getting the databases out of your data's way::

   import dataset

   db = dataset.connect('sqlite:///weather.db')
   db['temperature'].find()

Features include:

* **Automatic schema**. If a table or column is written that does not
  exist in the database, it will be created automatically.
* **Upserts**. Records are either created or updated, depdending on
  whether an existing version can be found.
* **Query helpers** for simple queries such as all rows in a table or
  all distinct values across a set of columns.

.. toctree::
   :maxdepth: 2

Next steps:

* Quickstart: `Learn how to use dataset in five minutes <quickstart>`_
* API: `Browse the complete API docs <api>`_



Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

