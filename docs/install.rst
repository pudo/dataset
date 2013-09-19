
Installation Guide
==================

*— work in progress —*

The easiest way is to install ``dataset`` from the `Python Package Index <https://pypi.python.org/pypi/dataset/>`_ using ``pip`` or ``easy_install``:

.. code-block:: bash

   $ pip install dataset

To install it manually simply download the repository from Github:

.. code-block:: bash

   $ git clone git://github.com/pudo/dataset.git
   $ cd dataset/
   $ python setup.py install

Depending on the type of database backend, you may also need to install a database specific driver package. For MySQL, this is ``MySQLdb``, for Postgres its ``psycopg2``. SQLite support is integrated into Python.