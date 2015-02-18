import logging
import threading
import re

import six
from six.moves.urllib.parse import urlencode, parse_qs

from sqlalchemy import create_engine
from sqlalchemy import Integer, String
from sqlalchemy.sql import text
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import MetaData, Column
from sqlalchemy.schema import Table as SQLATable
from sqlalchemy.util import safe_reraise

from alembic.migration import MigrationContext
from alembic.operations import Operations

from dataset.persistence.table import Table
from dataset.persistence.util import ResultIter
from dataset.util import DatasetException

log = logging.getLogger(__name__)


class Database(object):
    def __init__(self, url, schema=None, reflectMetadata=True,
                 engine_kwargs=None, reflect_views=True):
        if engine_kwargs is None:
            engine_kwargs = {}

        if url.startswith('postgres'):
            engine_kwargs.setdefault('poolclass', NullPool)

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
        self.engine = create_engine(url, **engine_kwargs)
        self.url = url
        self.metadata = MetaData(schema=schema)
        self.metadata.bind = self.engine
        if reflectMetadata:
            self.metadata.reflect(self.engine, views=reflect_views)
        self._tables = {}

    @property
    def executable(self):
        """ The current connection or engine against which statements
        will be executed. """
        if hasattr(self.local, 'connection'):
            return self.local.connection
        return self.engine

    @property
    def op(self):
        ctx = MigrationContext.configure(self.engine)
        return Operations(ctx)

    def _acquire(self):
        self.lock.acquire()

    def _release(self):
        if not hasattr(self.local, 'tx'):
            self.lock.release()
            self.local.must_release = False
        else:
            self.local.must_release = True

    def _release_internal(self):
        if getattr(self.local, 'must_release', None):
            self.lock.release()
            self.local.must_release = False

    def _dispose_transaction(self):
        self.local.tx.remove(self.local.tx[-1])
        if not self.local.tx:
            del self.local.tx
            self.local.connection.close()
            del self.local.connection
            self._release_internal()

    def begin(self):
        """ Enter a transaction explicitly. No data will be written
        until the transaction has been committed.

        **NOTICE:** Schema modification operations, such as the creation
        of tables or columns will not be part of the transactional context."""
        if not hasattr(self.local, 'connection'):
            self.local.connection = self.engine.connect()
        if not hasattr(self.local, 'tx'):
            self.local.tx = []
        self.local.tx.append(self.local.connection.begin())

    def commit(self):
        """ Commit the current transaction, making all statements executed
        since the transaction was begun permanent. """
        if hasattr(self.local, 'tx') and self.local.tx:
            self.local.tx[-1].commit()
            self._dispose_transaction()

    def rollback(self):
        """ Roll back the current transaction, discarding all statements
        executed since the transaction was begun. """
        if hasattr(self.local, 'tx') and self.local.tx:
            self.local.tx[-1].rollback()
            self._dispose_transaction()

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, error_type, error_value, traceback):
        if error_type is None:
            try:
                self.commit()
            except:
                with safe_reraise():
                    self.rollback()
        else:
            self.rollback()

    @property
    def tables(self):
        """
        Get a listing of all tables that exist in the database.
        """
        return list(
            set(self.metadata.tables.keys()) | set(self._tables.keys())
        )

    def __contains__(self, member):
        return member in self.tables

    def create_table(self, table_name, primary_id='id', primary_type='Integer'):
        """
        Creates a new table. The new table will automatically have an `id` column
        unless specified via optional parameter primary_id, which will be used
        as the primary key of the table. Automatic id is set to be an
        auto-incrementing integer, while the type of custom primary_id can be a
        String or an Integer as specified with primary_type flag. The default
        length of String is 255. The caller can specify the length.
        The caller will be responsible for the uniqueness of manual primary_id.

        This custom id feature is only available via direct create_table call.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.create_table('population')

            # custom id and type
            table2 = db.create_table('population2', 'age')
            table3 = db.create_table('population3', primary_id='race', primary_type='String')
            # custom length of String
            table4 = db.create_table('population4', primary_id='race', primary_type='String(50)')
        """
        self._acquire()
        try:
            log.debug("Creating table: %s on %r" % (table_name, self.engine))
            match = re.match(r'^(Integer)$|^(String)(\(\d+\))?$', primary_type)
            if match:
                if match.group(1) == 'Integer':
                    auto_flag = False
                    if primary_id == 'id':
                        auto_flag = True
                    col = Column(primary_id, Integer, primary_key=True, autoincrement=auto_flag)
                elif not match.group(3):
                    col = Column(primary_id, String(255), primary_key=True)
                else:
                    len_string = int(match.group(3)[1:-1])
                    len_string = min(len_string, 255)
                    col = Column(primary_id, String(len_string), primary_key=True)
            else:
                raise DatasetException(
                    "The primary_type has to be either 'Integer' or 'String'.")

            table = SQLATable(table_name, self.metadata)
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

    def update_table(self, table_name):
        self.metadata = MetaData(schema=self.schema)
        self.metadata.bind = self.engine
        self.metadata.reflect(self.engine)
        self._tables[table_name] = SQLATable(table_name, self.metadata)
        return self._tables[table_name]

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
        return self.get_table(table_name)

    def query(self, query, **kw):
        """
        Run a statement on the database directly, allowing for the
        execution of arbitrary read/write queries. A query can either be
        a plain text string, or a `SQLAlchemy expression <http://docs.sqlalchemy.org/en/latest/core/tutorial.html#selecting>`_.
        If a plain string is passed in, it will be converted to an expression automatically.

        Keyword arguments will be used for parameter binding. See the `SQLAlchemy
        documentation <http://docs.sqlalchemy.org/en/rel_0_9/core/connections.html#sqlalchemy.engine.Connection.execute>`_ for details.

        The returned iterator will yield each result sequentially.
        ::

            res = db.query('SELECT user, COUNT(*) c FROM photos GROUP BY user')
            for row in res:
                print(row['user'], row['c'])
        """
        if isinstance(query, six.string_types):
            query = text(query)
        return ResultIter(self.executable.execute(query, **kw))

    def __repr__(self):
        return '<Database(%s)>' % self.url
