import logging
import threading
from urlparse import parse_qs
from urllib import urlencode

from sqlalchemy import create_engine
from migrate.versioning.util import construct_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import MetaData, Column, Index
from sqlalchemy.schema import Table as SQLATable
from sqlalchemy import Integer, Text

from dataset.persistence.table import Table
from dataset.persistence.util import ResultIter
from dataset.util import DatasetException

log = logging.getLogger(__name__)


class Database(object):

    def __init__(self, url, schema=None, reflectMetadata=True):
        kw = {}
        if url.startswith('postgres'):
            kw['poolclass'] = NullPool
        self.lock = threading.RLock()
        self.local = threading.local()
        if '?' in url:
            url, query = url.split('?', 1)
            query = parse_qs(query)
            if schema is None:
                # le pop
                schema_qs = query.pop('schema', query.pop('searchpath', []))
                if len(schema_qs):
                    schema = schema_qs.pop()
            if len(query):
                url = url + '?' + urlencode(query, doseq=True)
        self.schema = schema
        engine = create_engine(url, **kw)
        self.url = url
        self.engine = construct_engine(engine)
        self.metadata = MetaData(schema=schema)
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

    def create_table(self, table_name, primary_id='id', primary_type='Integer'):
        """
        Creates a new table. The new table will automatically have an `id` column 
        unless specified via optional parameter primary_id, which will be used 
        as the primary key of the table. Automatic id is set to be an 
        auto-incrementing integer, while the type of custom primary_id can be a 
        Text or an Integer as specified with primary_type flag. 
        The caller will be responsible for the uniqueness of manual primary_id.

        This custom id feature is only available via direct create_table call. 

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.create_table('population')

            # custom id and type
            table2 = db.create_table('population2', 'age')
            table3 = db.create_table('population3', primary_id='race', primary_type='Text')
        """
        self._acquire()
        try:
            log.debug("Creating table: %s on %r" % (table_name, self.engine))
            table = SQLATable(table_name, self.metadata)
            if primary_type == 'Integer':
                auto_flag = False
                if primary_id == 'id':
                    auto_flag = True
                col = Column(primary_id, Integer, primary_key=True, autoincrement=auto_flag)
            elif primary_type == 'Text':
                col = Column(primary_id, Text, primary_key=True)
            else:
                raise DatasetException(
                    "The primary_type has to be either 'Integer' or 'Text'.")

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

    def get_table(self, table_name, primary_id='id', primary_type='Integer'):
        """
        Smart wrapper around *load_table* and *create_table*. Either loads a table
        or creates it if it doesn't exist yet.
        For short-hand to create a table with custom id and type using [], where
        table_name, primary_id, and primary_type are specified as a tuple

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.get_table('population')
            # you can also use the short-hand syntax:
            table = db['population']

            # custom id and type
            table2 = db['population2', 'age'] # default type is 'Integer'
            table3 = db['population3', 'race', 'Text']
        """
        if table_name in self._tables:
            return Table(self, self._tables[table_name])
        self._acquire()
        try:
            if self.engine.has_table(table_name, schema=self.schema):
                return self.load_table(table_name)
            else:
                return self.create_table(table_name, primary_id, primary_type)
        finally:
            self._release()

    def __getitem__(self, table_name):
        if type(table_name) is tuple:
            return self.get_table(*table_name[:3])
        else:
            return self.get_table(table_name)

    def query(self, query, **kw):
        """
        Run a statement on the database directly, allowing for the
        execution of arbitrary read/write queries. A query can either be
        a plain text string, or a `SQLAlchemy expression <http://docs.sqlalchemy.org/ru/latest/core/tutorial.html#selecting>`_. The returned
        iterator will yield each result sequentially.

        Any keyword arguments will be passed into the query to perform
        parameter binding. 
        ::

            res = db.query('SELECT user, COUNT(*) c FROM photos GROUP BY user')
            for row in res:
                print row['user'], row['c']
        """
        return ResultIter(self.executable.execute(query, **kw))

    def __repr__(self):
        return '<Database(%s)>' % self.url
