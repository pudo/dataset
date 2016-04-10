import logging
import threading
import re

import six
from six.moves.urllib.parse import parse_qs, urlparse

from sqlalchemy import create_engine
from sqlalchemy import Integer, String
from sqlalchemy.sql import text
from sqlalchemy.schema import MetaData, Column
from sqlalchemy.schema import Table as SQLATable
from sqlalchemy.pool import StaticPool
from sqlalchemy.util import safe_reraise

from alembic.migration import MigrationContext
from alembic.operations import Operations

from dataset.persistence.table import Table
from dataset.persistence.util import ResultIter, row_type, safe_url
from dataset.util import DatasetException

log = logging.getLogger(__name__)


class Database(object):
    """A database object represents a SQL database with multiple tables."""

    def __init__(self, url, schema=None, reflect_metadata=True,
                 engine_kwargs=None, reflect_views=True,
                 ensure_schema=True, row_type=row_type):
        """Configure and connect to the database."""
        if engine_kwargs is None:
            engine_kwargs = {}

        parsed_url = urlparse(url)
        if parsed_url.scheme.lower() in 'sqlite':
            # ref: https://github.com/pudo/dataset/issues/163
            if 'poolclass' not in engine_kwargs:
                engine_kwargs['poolclass'] = StaticPool

        self.lock = threading.RLock()
        self.local = threading.local()
        if len(parsed_url.query):
            query = parse_qs(parsed_url.query)
            if schema is None:
                schema_qs = query.get('schema', query.get('searchpath', []))
                if len(schema_qs):
                    schema = schema_qs.pop()

        self.schema = schema
        self.engine = create_engine(url, **engine_kwargs)
        self.url = url
        self.row_type = row_type
        self.ensure_schema = ensure_schema
        self._tables = {}
        self.metadata = MetaData(schema=schema)
        self.metadata.bind = self.engine
        if reflect_metadata:
            self.metadata.reflect(self.engine, views=reflect_views)
            for table_name in self.metadata.tables.keys():
                self.load_table(self.metadata.tables[table_name].name)

    @property
    def executable(self):
        """Connection or engine against which statements will be executed."""
        if hasattr(self.local, 'connection'):
            return self.local.connection
        return self.engine

    @property
    def op(self):
        """Get an alembic operations context."""
        ctx = MigrationContext.configure(self.engine)
        return Operations(ctx)

    def _acquire(self):
        self.lock.acquire()

    def _release(self):
        if not hasattr(self.local, 'tx'):
            self.lock.release()
        else:
            self.local.lock_count[-1] += 1

    def _release_internal(self):
        for index in range(self.local.lock_count[-1]):
            self.lock.release()
        del self.local.lock_count[-1]

    def _dispose_transaction(self):
        self._release_internal()
        self.local.tx.remove(self.local.tx[-1])
        if not self.local.tx:
            del self.local.tx
            del self.local.lock_count
            self.local.connection.close()
            del self.local.connection

    def begin(self):
        """
        Enter a transaction explicitly.

        No data will be written until the transaction has been committed.
        **NOTICE:** Schema modification operations, such as the creation
        of tables or columns will not be part of the transactional context.
        """
        if not hasattr(self.local, 'connection'):
            self.local.connection = self.engine.connect()
        if not hasattr(self.local, 'tx'):
            self.local.tx = []
            self.local.lock_count = []
        self.local.tx.append(self.local.connection.begin())
        self.local.lock_count.append(0)

    def commit(self):
        """
        Commit the current transaction.

        Make all statements executed since the transaction was begun permanent.
        """
        if hasattr(self.local, 'tx') and self.local.tx:
            self.local.tx[-1].commit()
            self._dispose_transaction()

    def rollback(self):
        """
        Roll back the current transaction.

        Discard all statements executed since the transaction was begun.
        """
        if hasattr(self.local, 'tx') and self.local.tx:
            self.local.tx[-1].rollback()
            self._dispose_transaction()

    def __enter__(self):
        """Start a transaction."""
        self.begin()
        return self

    def __exit__(self, error_type, error_value, traceback):
        """End a transaction by committing or rolling back."""
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
        """Get a listing of all tables that exist in the database."""
        return list(self._tables.keys())

    def __contains__(self, member):
        """Check if the given table name exists in the database."""
        return member in self.tables

    def _valid_table_name(self, table_name):
        """Check if the table name is obviously invalid."""
        if table_name is None or not len(table_name.strip()):
            raise ValueError("Invalid table name: %r" % table_name)
        return table_name.strip()

    def create_table(self, table_name, primary_id='id', primary_type='Integer'):
        """
        Create a new table.

        The new table will automatically have an `id` column unless specified via
        optional parameter primary_id, which will be used as the primary key of the
        table. Automatic id is set to be an auto-incrementing integer, while the
        type of custom primary_id can be a
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
        table_name = self._valid_table_name(table_name)
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

            table = SQLATable(table_name, self.metadata, schema=self.schema)
            table.append_column(col)
            table.create(self.engine)
            self._tables[table_name] = table
            return Table(self, table)
        finally:
            self._release()

    def load_table(self, table_name):
        """
        Load a table.

        This will fail if the tables does not already exist in the database. If the
        table exists, its columns will be reflected and are available on the
        :py:class:`Table <dataset.Table>` object.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.load_table('population')
        """
        table_name = self._valid_table_name(table_name)
        self._acquire()
        try:
            log.debug("Loading table: %s on %r" % (table_name, self))
            table = SQLATable(table_name, self.metadata,
                              schema=self.schema, autoload=True)
            self._tables[table_name] = table
            return Table(self, table)
        finally:
            self._release()

    def update_table(self, table_name):
        """Reload a table schema from the database."""
        table_name = self._valid_table_name(table_name)
        self.metadata = MetaData(schema=self.schema)
        self.metadata.bind = self.engine
        self.metadata.reflect(self.engine)
        self._tables[table_name] = SQLATable(table_name, self.metadata,
                                             schema=self.schema)
        return self._tables[table_name]

    def get_table(self, table_name, primary_id='id', primary_type='Integer'):
        """
        Smart wrapper around *load_table* and *create_table*.

        Either loads a table or creates it if it doesn't exist yet.
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
        """Get a given table."""
        return self.get_table(table_name)

    def query(self, query, **kw):
        """
        Run a statement on the database directly.

        Allows for the execution of arbitrary read/write queries. A query can either be
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
        return ResultIter(self.executable.execute(query, **kw),
                          row_type=self.row_type)

    def __repr__(self):
        """Text representation contains the URL."""
        return '<Database(%s)>' % safe_url(self.url)
