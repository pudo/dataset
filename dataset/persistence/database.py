import logging
from threading import RLock

from sqlalchemy import create_engine
from migrate.versioning.util import construct_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import MetaData, Column, Index
from sqlalchemy.schema import Table as SQLATable
from sqlalchemy import Integer

from dataset.persistence.table import Table
from dataset.persistence.util import ResultIter


log = logging.getLogger(__name__)


class Database(object):

    def __init__(self, url):
        kw = {}
        if url.startswith('postgres'):
            kw['poolclass'] = NullPool
        engine = create_engine(url, **kw)
        self.lock = RLock()
        self.url = url
        self.engine = construct_engine(engine)
        self.metadata = MetaData()
        self.metadata.bind = self.engine
        self.metadata.reflect(self.engine)
        self._tables = {}

    @property
    def tables(self):
        """ Get a listing of all tables that exist in the database.

        >>> print db.tables
        set([u'user', u'action'])
        """
        return list(set(self.metadata.tables.keys() +
            self._tables.keys()))

    def create_table(self, table_name):
        """
        Creates a new table. The new table will automatically have an `id` column, which is
        set to be an auto-incrementing integer as the primary key of the table.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.create_table('population')
        """
        with self.lock:
            log.debug("Creating table: %s on %r" % (table_name, self.engine))
            table = SQLATable(table_name, self.metadata)
            col = Column('id', Integer, primary_key=True)
            table.append_column(col)
            table.create(self.engine)
            self._tables[table_name] = table
            return Table(self, table)

    def load_table(self, table_name):
        """
        Loads a table. This will fail if the tables does not already
        exist in the database. If the table exists, its columns will be
        reflected and are available on the :py:class:`Table <dataset.Table>`
        object.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.load_table('population')
        """
        with self.lock:
            log.debug("Loading table: %s on %r" % (table_name, self))
            table = SQLATable(table_name, self.metadata, autoload=True)
            self._tables[table_name] = table
            return Table(self, table)

    def get_table(self, table_name):
        """
        Smart wrapper around *load_table* and *create_table*. Either loads a table
        or creates it if it doesn't exist yet.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.get_table('population')
            # you can also use the short-hand syntax:
            table = db['population']
        """
        with self.lock:
            if table_name in self._tables:
                return Table(self, self._tables[table_name])
            if self.engine.has_table(table_name):
                return self.load_table(table_name)
            else:
                return self.create_table(table_name)

    def __getitem__(self, table_name):
        return self.get_table(table_name)

    def query(self, query):
        """
        Run a statement on the database directly, allowing for the
        execution of arbitrary read/write queries. A query can either be
        a plain text string, or a `SQLAlchemy expression <http://docs.sqlalchemy.org/ru/latest/core/tutorial.html#selecting>`_. The returned
        iterator will yield each result sequentially.
        ::

            res = db.query('SELECT user, COUNT(*) c FROM photos GROUP BY user')
            for row in res:
                print row['user'], row['c']
        """
        return ResultIter(self.engine.execute(query))

    def __repr__(self):
        return '<Database(%s)>' % self.url

