import logging
import threading

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

    def __init__(self, url, reflectMetadata=True):
        kw = {}
        if url.startswith('postgres'):
            kw['poolclass'] = NullPool
        engine = create_engine(url, **kw)
        self.lock = threading.RLock()
        self.local = threading.local()
        self.url = url
        self.engine = construct_engine(engine)
        self.metadata = MetaData()
        self.metadata.bind = self.engine
        if reflectMetadata:
            self.metadata.reflect(self.engine)
        self._tables = {}

    @property
    def executable(self):
        """ The current connection or engine against which statements
        will be executed. """
        if hasattr(self.local, 'connection'):
            return self.local.connection
        return self.engine

    def _acquire(self):
        self.lock.acquire()

    def _release(self):
        if not hasattr(self.local, 'tx'):
            self.lock.release()
            self.local.must_release = False
        else:
            self.local.must_release = True

    def _release_internal(self):
        if not hasattr(self.local, 'must_release') and self.local.must_release:
            self.lock.release()
            self.local.must_release = False

    def begin(self):
        """ Enter a transaction explicitly. No data will be written
        until the transaction has been committed. """
        if not hasattr(self.local, 'connection'):
            self.local.connection = self.engine.connect()
        if not hasattr(self.local, 'tx'):
            self.local.tx = self.local.connection.begin()

    def commit(self):
        """ Commit the current transaction, making all statements executed
        since the transaction was begun permanent. """
        self.local.tx.commit()
        del self.local.tx
        self._release_internal()

    def rollback(self):
        """ Roll back the current transaction, discarding all statements
        executed since the transaction was begun. """
        self.local.tx.rollback()
        del self.local.tx
        self._release_internal()

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
        self._acquire()
        try:
            log.debug("Creating table: %s on %r" % (table_name, self.engine))
            table = SQLATable(table_name, self.metadata)
            col = Column('id', Integer, primary_key=True)
            table.append_column(col)
            table.create(self.engine)
            self._tables[table_name] = table
            return Table(self, table)
        finally:
            self._release()

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
        self._acquire()
        try:
            log.debug("Loading table: %s on %r" % (table_name, self))
            table = SQLATable(table_name, self.metadata, autoload=True)
            self._tables[table_name] = table
            return Table(self, table)
        finally:
            self._release()

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
        if table_name in self._tables:
            return Table(self, self._tables[table_name])
        self._acquire()
        try:
            if self.engine.has_table(table_name):
                return self.load_table(table_name)
            else:
                return self.create_table(table_name)
        finally:
            self._release()

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
        return ResultIter(self.executable.execute(query))

    def __repr__(self):
        return '<Database(%s)>' % self.url

