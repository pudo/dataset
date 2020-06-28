import logging
import threading
from urllib.parse import parse_qs, urlparse

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.schema import MetaData
from sqlalchemy.util import safe_reraise
from sqlalchemy.engine.reflection import Inspector

from alembic.migration import MigrationContext
from alembic.operations import Operations

from dataset.table import Table
from dataset.util import ResultIter, row_type, safe_url, QUERY_STEP
from dataset.util import normalize_table_name
from dataset.types import Types

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
        # if parsed_url.scheme.lower() in 'sqlite':
        #     # ref: https://github.com/pudo/dataset/issues/163
        #     if 'poolclass' not in engine_kwargs:
        #         engine_kwargs['poolclass'] = StaticPool

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
        self.types = Types(self.engine.dialect.name)
        self.url = url
        self.row_type = row_type
        self.ensure_schema = ensure_schema
        self._tables = {}

    @property
    def executable(self):
        """Connection against which statements will be executed."""
        if not hasattr(self.local, 'conn'):
            self.local.conn = self.engine.connect()
        return self.local.conn

    @property
    def op(self):
        """Get an alembic operations context."""
        ctx = MigrationContext.configure(self.executable)
        return Operations(ctx)

    @property
    def inspect(self):
        """Get a SQLAlchemy inspector."""
        return Inspector.from_engine(self.executable)

    @property
    def metadata(self):
        """Return a SQLAlchemy schema cache object."""
        return MetaData(schema=self.schema, bind=self.executable)

    @property
    def in_transaction(self):
        """Check if this database is in a transactional context."""
        if not hasattr(self.local, 'tx'):
            return False
        return len(self.local.tx) > 0

    def _flush_tables(self):
        """Clear the table metadata after transaction rollbacks."""
        for table in self._tables.values():
            table._table = None

    def begin(self):
        """Enter a transaction explicitly.

        No data will be written until the transaction has been committed.
        """
        if not hasattr(self.local, 'tx'):
            self.local.tx = []
        self.local.tx.append(self.executable.begin())

    def commit(self):
        """Commit the current transaction.

        Make all statements executed since the transaction was begun permanent.
        """
        if hasattr(self.local, 'tx') and self.local.tx:
            tx = self.local.tx.pop()
            tx.commit()
            self._flush_tables()

    def rollback(self):
        """Roll back the current transaction.

        Discard all statements executed since the transaction was begun.
        """
        if hasattr(self.local, 'tx') and self.local.tx:
            tx = self.local.tx.pop()
            tx.rollback()
            self._flush_tables()

    def __enter__(self):
        """Start a transaction."""
        self.begin()
        return self

    def __exit__(self, error_type, error_value, traceback):
        """End a transaction by committing or rolling back."""
        if error_type is None:
            try:
                self.commit()
            except Exception:
                with safe_reraise():
                    self.rollback()
        else:
            self.rollback()

    def close(self):
        """Close database connections. Makes this object unusable."""
        self.engine.dispose()
        self._tables = {}
        self.engine = None

    @property
    def tables(self):
        """Get a listing of all tables that exist in the database."""
        return self.inspect.get_table_names(schema=self.schema)

    @property
    def views(self):
        """Get a listing of all views that exist in the database."""
        return self.inspect.get_view_names(schema=self.schema)

    def __contains__(self, table_name):
        """Check if the given table name exists in the database."""
        try:
            table_name = normalize_table_name(table_name)
            if table_name in self.tables:
                return True
            if table_name in self.views:
                return True
            return False
        except ValueError:
            return False

    def create_table(self, table_name, primary_id=None, primary_type=None):
        """Create a new table.

        Either loads a table or creates it if it doesn't exist yet. You can
        define the name and type of the primary key field, if a new table is to
        be created. The default is to create an auto-incrementing integer,
        ``id``. You can also set the primary key to be a string or big integer.
        The caller will be responsible for the uniqueness of ``primary_id`` if
        it is defined as a text type.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.create_table('population')

            # custom id and type
            table2 = db.create_table('population2', 'age')
            table3 = db.create_table('population3',
                                     primary_id='city',
                                     primary_type=db.types.text)
            # custom length of String
            table4 = db.create_table('population4',
                                     primary_id='city',
                                     primary_type=db.types.string(25))
            # no primary key
            table5 = db.create_table('population5',
                                     primary_id=False)
        """
        assert not isinstance(primary_type, str), \
            'Text-based primary_type support is dropped, use db.types.'
        table_name = normalize_table_name(table_name)
        with self.lock:
            if table_name not in self._tables:
                self._tables[table_name] = Table(self, table_name,
                                                 primary_id=primary_id,
                                                 primary_type=primary_type,
                                                 auto_create=True)
            return self._tables.get(table_name)

    def load_table(self, table_name):
        """Load a table.

        This will fail if the tables does not already exist in the database. If
        the table exists, its columns will be reflected and are available on
        the :py:class:`Table <dataset.Table>` object.

        Returns a :py:class:`Table <dataset.Table>` instance.
        ::

            table = db.load_table('population')
        """
        table_name = normalize_table_name(table_name)
        with self.lock:
            if table_name not in self._tables:
                self._tables[table_name] = Table(self, table_name)
            return self._tables.get(table_name)

    def get_table(self, table_name, primary_id=None, primary_type=None):
        """Load or create a table.

        This is now the same as ``create_table``.
        ::

            table = db.get_table('population')
            # you can also use the short-hand syntax:
            table = db['population']
        """
        if not self.ensure_schema:
            return self.load_table(table_name)
        return self.create_table(table_name, primary_id, primary_type)

    def __getitem__(self, table_name):
        """Get a given table."""
        return self.get_table(table_name)

    def _ipython_key_completions_(self):
        """Completion for table names with IPython."""
        return self.tables

    def query(self, query, *args, **kwargs):
        """Run a statement on the database directly.

        Allows for the execution of arbitrary read/write queries. A query can
        either be a plain text string, or a `SQLAlchemy expression
        <http://docs.sqlalchemy.org/en/latest/core/tutorial.html#selecting>`_.
        If a plain string is passed in, it will be converted to an expression
        automatically.

        Further positional and keyword arguments will be used for parameter
        binding. To include a positional argument in your query, use question
        marks in the query (i.e. ``SELECT * FROM tbl WHERE a = ?```). For
        keyword arguments, use a bind parameter (i.e. ``SELECT * FROM tbl
        WHERE a = :foo``).
        ::

            statement = 'SELECT user, COUNT(*) c FROM photos GROUP BY user'
            for row in db.query(statement):
                print(row['user'], row['c'])

        The returned iterator will yield each result sequentially.
        """
        if isinstance(query, str):
            query = text(query)
        _step = kwargs.pop('_step', QUERY_STEP)
        if _step is False or _step == 0:
            _step = None
        rp = self.executable.execute(query, *args, **kwargs)
        return ResultIter(rp, row_type=self.row_type, step=_step)

    def __repr__(self):
        """Text representation contains the URL."""
        return '<Database(%s)>' % safe_url(self.url)
