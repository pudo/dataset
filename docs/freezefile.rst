
Freezefiles and the ``datafreeze`` command
==========================================

``datafreeze`` creates static extracts of SQL databases for use in interactive
web applications. SQL databases are a great way to manage relational data, but
exposing them on the web to drive data apps can be cumbersome. Often, the
capacities of a proper database are not actually required, a few static JSON
files and a bit of JavaScript can have the same effect. Still, exporting JSON
by hand (or with a custom script) can also become a messy process.

With ``datafreeze``, exports are scripted in a Makefile-like description, making them simple to repeat and replicate.


Basic Usage
-----------

Calling DataFreeze is simple, the application is called with a
freeze file as its argument:

.. code-block:: bash

    datafreeze Freezefile.yaml

Freeze files can be either written in JSON or in YAML. The database URI 
indicated in the Freezefile can also be overridden via the command line:

    datafreeze --db sqlite:///foo.db Freezefile.yaml


Example Freezefile.yaml
-----------------------

A freeze file is composed of a set of scripted queries and
specifications on how their output is to be handled. An example could look
like this:

.. code-block:: yaml

    common:

      database: "postgresql://user:password@localhost/operational_database"
      prefix: my_project/dumps/
      format: json

    exports:

      - query: "SELECT id, title, date FROM events"
        filename: "index.json"
      
      - query: "SELECT id, title, date, country FROM events"
        filename: "countries/{{country}}.csv"
        format: csv

      - query: "SELECT * FROM events"
        filename: "events/{{id}}.json"
        mode: item

      - query: "SELECT * FROM events"
        filename: "all.json"
        format: tabson

An identical JSON configuration can be found in this repository.


Options in detail
-----------------

The freeze file has two main sections, ``common`` and ``exports``. Both
accept many of the same arguments, with ``exports`` specifying a list of 
exports while ``common`` defines some shared properties, such as the 
database connection string.

The following options are recognized: 

* ``database`` is a database URI, including the database type, username 
  and password, hostname and database name. Valid database types include 
  ``sqlite``, ``mysql`` and ``postgresql`` (requires psycopg2).
* ``prefix`` specifies a common root directory for all extracted files.
* ``format`` identifies the format to be generated, ``csv``, ``json`` and
  ``tabson`` are supported. ``tabson`` is a condensed JSON
  representation in which rows are not represented by objects but by
  lists of values.
* ``query`` needs to be a valid SQL statement. All selected fields will
  become keys or columns in the output, so it may make sense to define 
  proper aliases if any overlap is to be expected.
* ``mode`` specifies whether the query output is to be combined into a 
  single file (``list``) or whether a file should be generated for each 
  result row (``item``).
* ``filename`` is the output file name, appended to ``prefix``. All
  occurences of ``{{field}}`` are expanded to a fields value to allow the
  generation of file names e.g. by primary key. In list mode, templating
  can be used to group records into several buckets, e.g. by country or
  category.
* ``wrap`` can be used to specify whether the output should be wrapped 
  in a ``results`` hash in JSON output. This defaults to ``true`` for 
  ``list``-mode output and ``false`` for ``item``-mode. 

