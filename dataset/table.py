import logging
import warnings
import threading
from banal import ensure_list

from sqlalchemy import func, select, false
from sqlalchemy.sql import and_, expression
from sqlalchemy.sql.expression import bindparam, ClauseElement
from sqlalchemy.schema import Column, Index
from sqlalchemy.schema import Table as SQLATable
from sqlalchemy.exc import NoSuchTableError

from dataset.types import Types, MYSQL_LENGTH_TYPES
from dataset.util import index_name
from dataset.util import DatasetException, ResultIter, QUERY_STEP
from dataset.util import normalize_table_name, pad_chunk_columns
from dataset.util import normalize_column_name, normalize_column_key


log = logging.getLogger(__name__)


class Table(object):
    """Represents a table in a database and exposes common operations."""

    PRIMARY_DEFAULT = "id"

    def __init__(
        self,
        database,
        table_name,
        primary_id=None,
        primary_type=None,
        primary_increment=None,
        auto_create=False,
    ):
        """Initialise the table from database schema."""
        self.db = database
        self.name = normalize_table_name(table_name)
        self._table = None
        self._columns = None
        self._indexes = []
        self._primary_id = (
            primary_id if primary_id is not None else self.PRIMARY_DEFAULT
        )
        self._primary_type = primary_type if primary_type is not None else Types.integer
        if primary_increment is None:
            primary_increment = self._primary_type in (Types.integer, Types.bigint)
        self._primary_increment = primary_increment
        self._auto_create = auto_create

    @property
    def exists(self):
        """Check to see if the table currently exists in the database."""
        if self._table is not None:
            return True
        return self.name in self.db

    @property
    def table(self):
        """Get a reference to the table, which may be reflected or created."""
        if self._table is None:
            self._sync_table(())
        return self._table

    @property
    def _column_keys(self):
        """Get a dictionary of all columns and their case mapping."""
        if not self.exists:
            return {}
        with self.db.lock:
            if self._columns is None:
                # Initialise the table if it doesn't exist
                table = self.table
                self._columns = {}
                for column in table.columns:
                    name = normalize_column_name(column.name)
                    key = normalize_column_key(name)
                    if key in self._columns:
                        log.warning("Duplicate column: %s", name)
                    self._columns[key] = name
            return self._columns

    @property
    def columns(self):
        """Get a listing of all columns that exist in the table."""
        return list(self._column_keys.values())

    def has_column(self, column):
        """Check if a column with the given name exists on this table."""
        key = normalize_column_key(normalize_column_name(column))
        return key in self._column_keys

    def _get_column_name(self, name):
        """Find the best column name with case-insensitive matching."""
        name = normalize_column_name(name)
        key = normalize_column_key(name)
        return self._column_keys.get(key, name)

    def insert(self, row, ensure=None, types=None):
        """Add a ``row`` dict by inserting it into the table.

        If ``ensure`` is set, any of the keys of the row are not
        table columns, they will be created automatically.

        During column creation, ``types`` will be checked for a key
        matching the name of a column to be created, and the given
        SQLAlchemy column type will be used. Otherwise, the type is
        guessed from the row value, defaulting to a simple unicode
        field.
        ::

            data = dict(title='I am a banana!')
            table.insert(data)

        Returns the inserted row's primary key.
        """
        row = self._sync_columns(row, ensure, types=types)
        res = self.db.executable.execute(self.table.insert(row))
        if len(res.inserted_primary_key) > 0:
            return res.inserted_primary_key[0]
        return True

    def insert_ignore(self, row, keys, ensure=None, types=None):
        """Add a ``row`` dict into the table if the row does not exist.

        If rows with matching ``keys`` exist no change is made.

        Setting ``ensure`` results in automatically creating missing columns,
        i.e., keys of the row are not table columns.

        During column creation, ``types`` will be checked for a key
        matching the name of a column to be created, and the given
        SQLAlchemy column type will be used. Otherwise, the type is
        guessed from the row value, defaulting to a simple unicode
        field.
        ::

            data = dict(id=10, title='I am a banana!')
            table.insert_ignore(data, ['id'])
        """
        row = self._sync_columns(row, ensure, types=types)
        if self._check_ensure(ensure):
            self.create_index(keys)
        args, _ = self._keys_to_args(row, keys)
        if self.count(**args) == 0:
            return self.insert(row, ensure=False)
        return False

    def insert_many(self, rows, chunk_size=1000, ensure=None, types=None):
        """Add many rows at a time.

        This is significantly faster than adding them one by one. Per default
        the rows are processed in chunks of 1000 per commit, unless you specify
        a different ``chunk_size``.

        See :py:meth:`insert() <dataset.Table.insert>` for details on
        the other parameters.
        ::

            rows = [dict(name='Dolly')] * 10000
            table.insert_many(rows)
        """
        # Sync table before inputting rows.
        sync_row = {}
        for row in rows:
            # Only get non-existing columns.
            sync_keys = list(sync_row.keys())
            for key in [k for k in row.keys() if k not in sync_keys]:
                # Get a sample of the new column(s) from the row.
                sync_row[key] = row[key]
        self._sync_columns(sync_row, ensure, types=types)

        # Get columns name list to be used for padding later.
        columns = sync_row.keys()

        chunk = []
        for index, row in enumerate(rows):
            chunk.append(row)

            # Insert when chunk_size is fulfilled or this is the last row
            if len(chunk) == chunk_size or index == len(rows) - 1:
                chunk = pad_chunk_columns(chunk, columns)
                self.table.insert().execute(chunk)
                chunk = []

    def update(self, row, keys, ensure=None, types=None, return_count=False):
        """Update a row in the table.

        The update is managed via the set of column names stated in ``keys``:
        they will be used as filters for the data to be updated, using the
        values in ``row``.
        ::

            # update all entries with id matching 10, setting their title
            # columns
            data = dict(id=10, title='I am a banana!')
            table.update(data, ['id'])

        If keys in ``row`` update columns not present in the table, they will
        be created based on the settings of ``ensure`` and ``types``, matching
        the behavior of :py:meth:`insert() <dataset.Table.insert>`.
        """
        row = self._sync_columns(row, ensure, types=types)
        args, row = self._keys_to_args(row, keys)
        clause = self._args_to_clause(args)
        if not len(row):
            return self.count(clause)
        stmt = self.table.update(whereclause=clause, values=row)
        rp = self.db.executable.execute(stmt)
        if rp.supports_sane_rowcount():
            return rp.rowcount
        if return_count:
            return self.count(clause)

    def update_many(self, rows, keys, chunk_size=1000, ensure=None, types=None):
        """Update many rows in the table at a time.

        This is significantly faster than updating them one by one. Per default
        the rows are processed in chunks of 1000 per commit, unless you specify
        a different ``chunk_size``.

        See :py:meth:`update() <dataset.Table.update>` for details on
        the other parameters.
        """
        keys = ensure_list(keys)

        chunk = []
        columns = []
        for index, row in enumerate(rows):
            chunk.append(row)
            for col in row.keys():
                if col not in columns:
                    columns.append(col)

            # bindparam requires names to not conflict (cannot be "id" for id)
            for key in keys:
                row["_%s" % key] = row[key]

            # Update when chunk_size is fulfilled or this is the last row
            if len(chunk) == chunk_size or index == len(rows) - 1:
                cl = [self.table.c[k] == bindparam("_%s" % k) for k in keys]
                stmt = self.table.update(
                    whereclause=and_(*cl),
                    values={col: bindparam(col, required=False) for col in columns},
                )
                self.db.executable.execute(stmt, chunk)
                chunk = []

    def upsert(self, row, keys, ensure=None, types=None):
        """An UPSERT is a smart combination of insert and update.

        If rows with matching ``keys`` exist they will be updated, otherwise a
        new row is inserted in the table.
        ::

            data = dict(id=10, title='I am a banana!')
            table.upsert(data, ['id'])
        """
        row = self._sync_columns(row, ensure, types=types)
        if self._check_ensure(ensure):
            self.create_index(keys)
        row_count = self.update(row, keys, ensure=False, return_count=True)
        if row_count == 0:
            return self.insert(row, ensure=False)
        return True

    def upsert_many(self, rows, keys, chunk_size=1000, ensure=None, types=None):
        """
        Sorts multiple input rows into upserts and inserts. Inserts are passed
        to insert and upserts are updated.

        See :py:meth:`upsert() <dataset.Table.upsert>` and
        :py:meth:`insert_many() <dataset.Table.insert_many>`.
        """
        # Removing a bulk implementation in 5e09aba401. Doing this one by one
        # is incredibly slow, but doesn't run into issues with column creation.
        for row in rows:
            self.upsert(row, keys, ensure=ensure, types=types)

    def delete(self, *clauses, **filters):
        """Delete rows from the table.

        Keyword arguments can be used to add column-based filters. The filter
        criterion will always be equality:
        ::

            table.delete(place='Berlin')

        If no arguments are given, all records are deleted.
        """
        if not self.exists:
            return False
        clause = self._args_to_clause(filters, clauses=clauses)
        stmt = self.table.delete(whereclause=clause)
        rp = self.db.executable.execute(stmt)
        return rp.rowcount > 0

    def _reflect_table(self):
        """Load the tables definition from the database."""
        with self.db.lock:
            self._columns = None
            try:
                self._table = SQLATable(
                    self.name, self.db.metadata, schema=self.db.schema, autoload=True
                )
            except NoSuchTableError:
                self._table = None

    def _threading_warn(self):
        if self.db.in_transaction and threading.active_count() > 1:
            warnings.warn(
                "Changing the database schema inside a transaction "
                "in a multi-threaded environment is likely to lead "
                "to race conditions and synchronization issues.",
                RuntimeWarning,
            )

    def _sync_table(self, columns):
        """Lazy load, create or adapt the table structure in the database."""
        if self._table is None:
            # Load an existing table from the database.
            self._reflect_table()
        if self._table is None:
            # Create the table with an initial set of columns.
            if not self._auto_create:
                raise DatasetException("Table does not exist: %s" % self.name)
            # Keep the lock scope small because this is run very often.
            with self.db.lock:
                self._threading_warn()
                self._table = SQLATable(
                    self.name, self.db.metadata, schema=self.db.schema
                )
                if self._primary_id is not False:
                    # This can go wrong on DBMS like MySQL and SQLite where
                    # tables cannot have no columns.
                    column = Column(
                        self._primary_id,
                        self._primary_type,
                        primary_key=True,
                        autoincrement=self._primary_increment,
                    )
                    self._table.append_column(column)
                for column in columns:
                    if not column.name == self._primary_id:
                        self._table.append_column(column)
                self._table.create(self.db.executable, checkfirst=True)
                self._columns = None
        elif len(columns):
            with self.db.lock:
                self._reflect_table()
                self._threading_warn()
                for column in columns:
                    if not self.has_column(column.name):
                        self.db.op.add_column(self.name, column, self.db.schema)
                self._reflect_table()

    def _sync_columns(self, row, ensure, types=None):
        """Create missing columns (or the table) prior to writes.

        If automatic schema generation is disabled (``ensure`` is ``False``),
        this will remove any keys from the ``row`` for which there is no
        matching column.
        """
        ensure = self._check_ensure(ensure)
        types = types or {}
        types = {self._get_column_name(k): v for (k, v) in types.items()}
        out = {}
        sync_columns = {}
        for name, value in row.items():
            name = self._get_column_name(name)
            if self.has_column(name):
                out[name] = value
            elif ensure:
                _type = types.get(name)
                if _type is None:
                    _type = self.db.types.guess(value)
                sync_columns[name] = Column(name, _type)
                out[name] = value
        self._sync_table(sync_columns.values())
        return out

    def _check_ensure(self, ensure):
        if ensure is None:
            return self.db.ensure_schema
        return ensure

    def _generate_clause(self, column, op, value):
        if op in ("like",):
            return self.table.c[column].like(value)
        if op in ("ilike",):
            return self.table.c[column].ilike(value)
        if op in ("notlike",):
            return self.table.c[column].notlike(value)
        if op in ("notilike",):
            return self.table.c[column].notilike(value)
        if op in (">", "gt"):
            return self.table.c[column] > value
        if op in ("<", "lt"):
            return self.table.c[column] < value
        if op in (">=", "gte"):
            return self.table.c[column] >= value
        if op in ("<=", "lte"):
            return self.table.c[column] <= value
        if op in ("=", "==", "is"):
            return self.table.c[column] == value
        if op in ("!=", "<>", "not"):
            return self.table.c[column] != value
        if op in ("in",):
            return self.table.c[column].in_(value)
        if op in ("notin",):
            return self.table.c[column].notin_(value)
        if op in ("between", ".."):
            start, end = value
            return self.table.c[column].between(start, end)
        if op in ("startswith",):
            return self.table.c[column].like("%" + value)
        if op in ("endswith",):
            return self.table.c[column].like(value + "%")
        return false()

    def _args_to_clause(self, args, clauses=()):
        clauses = list(clauses)
        for column, value in args.items():
            column = self._get_column_name(column)
            if not self.has_column(column):
                clauses.append(false())
            elif isinstance(value, (list, tuple, set)):
                clauses.append(self._generate_clause(column, "in", value))
            elif isinstance(value, dict):
                for op, op_value in value.items():
                    clauses.append(self._generate_clause(column, op, op_value))
            else:
                clauses.append(self._generate_clause(column, "=", value))
        return and_(*clauses)

    def _args_to_order_by(self, order_by):
        orderings = []
        for ordering in ensure_list(order_by):
            if ordering is None:
                continue
            column = ordering.lstrip("-")
            column = self._get_column_name(column)
            if not self.has_column(column):
                continue
            if ordering.startswith("-"):
                orderings.append(self.table.c[column].desc())
            else:
                orderings.append(self.table.c[column].asc())
        return orderings

    def _keys_to_args(self, row, keys):
        keys = [self._get_column_name(k) for k in ensure_list(keys)]
        row = row.copy()
        args = {k: row.pop(k, None) for k in keys}
        return args, row

    def create_column(self, name, type, **kwargs):
        """Create a new column ``name`` of a specified type.
        ::

            table.create_column('created_at', db.types.datetime)

        `type` corresponds to an SQLAlchemy type as described by
        `dataset.db.Types`. Additional keyword arguments are passed
        to the constructor of `Column`, so that default values, and
        options like `nullable` and `unique` can be set.
        ::

            table.create_column('key', unique=True, nullable=False)
            table.create_column('food', default='banana')
        """
        name = self._get_column_name(name)
        if self.has_column(name):
            log.debug("Column exists: %s" % name)
            return
        self._sync_table((Column(name, type, **kwargs),))

    def create_column_by_example(self, name, value):
        """
        Explicitly create a new column ``name`` with a type that is appropriate
        to store the given example ``value``.  The type is guessed in the same
        way as for the insert method with ``ensure=True``.
        ::

            table.create_column_by_example('length', 4.2)

        If a column of the same name already exists, no action is taken, even
        if it is not of the type we would have created.
        """
        type_ = self.db.types.guess(value)
        self.create_column(name, type_)

    def drop_column(self, name):
        """
        Drop the column ``name``.
        ::

            table.drop_column('created_at')

        """
        if self.db.engine.dialect.name == "sqlite":
            raise RuntimeError("SQLite does not support dropping columns.")
        name = self._get_column_name(name)
        with self.db.lock:
            if not self.exists or not self.has_column(name):
                log.debug("Column does not exist: %s", name)
                return

            self._threading_warn()
            self.db.op.drop_column(self.table.name, name, self.table.schema)
            self._reflect_table()

    def drop(self):
        """Drop the table from the database.

        Deletes both the schema and all the contents within it.
        """
        with self.db.lock:
            if self.exists:
                self._threading_warn()
                self.table.drop(self.db.executable, checkfirst=True)
                self._table = None
                self._columns = None
                self.db._tables.pop(self.name, None)

    def has_index(self, columns):
        """Check if an index exists to cover the given ``columns``."""
        if not self.exists:
            return False
        columns = set([self._get_column_name(c) for c in columns])
        if columns in self._indexes:
            return True
        for column in columns:
            if not self.has_column(column):
                return False
        indexes = self.db.inspect.get_indexes(self.name, schema=self.db.schema)
        for index in indexes:
            if columns == set(index.get("column_names", [])):
                self._indexes.append(columns)
                return True
        return False

    def create_index(self, columns, name=None, **kw):
        """Create an index to speed up queries on a table.

        If no ``name`` is given a random name is created.
        ::

            table.create_index(['name', 'country'])
        """
        columns = [self._get_column_name(c) for c in ensure_list(columns)]
        with self.db.lock:
            if not self.exists:
                raise DatasetException("Table has not been created yet.")

            for column in columns:
                if not self.has_column(column):
                    return

            if not self.has_index(columns):
                self._threading_warn()
                name = name or index_name(self.name, columns)
                columns = [self.table.c[c] for c in columns]

                # MySQL crashes out if you try to index very long text fields,
                # apparently. This defines (a somewhat random) prefix that
                # will be captured by the index, after which I assume the engine
                # conducts a more linear scan:
                mysql_length = {}
                for col in columns:
                    if isinstance(col.type, MYSQL_LENGTH_TYPES):
                        mysql_length[col.name] = 10
                kw["mysql_length"] = mysql_length

                idx = Index(name, *columns, **kw)
                idx.create(self.db.executable)

    def find(self, *_clauses, **kwargs):
        """Perform a simple search on the table.

        Simply pass keyword arguments as ``filter``.
        ::

            results = table.find(country='France')
            results = table.find(country='France', year=1980)

        Using ``_limit``::

            # just return the first 10 rows
            results = table.find(country='France', _limit=10)

        You can sort the results by single or multiple columns. Append a minus
        sign to the column name for descending order::

            # sort results by a column 'year'
            results = table.find(country='France', order_by='year')
            # return all rows sorted by multiple columns (descending by year)
            results = table.find(order_by=['country', '-year'])

        You can also submit filters based on criteria other than equality,
        see :ref:`advanced_filters` for details.

        To run more complex queries with JOINs, or to perform GROUP BY-style
        aggregation, you can also use :py:meth:`db.query() <dataset.Database.query>`
        to run raw SQL queries instead.
        """
        if not self.exists:
            return iter([])

        _limit = kwargs.pop("_limit", None)
        _offset = kwargs.pop("_offset", 0)
        order_by = kwargs.pop("order_by", None)
        _streamed = kwargs.pop("_streamed", False)
        _step = kwargs.pop("_step", QUERY_STEP)
        if _step is False or _step == 0:
            _step = None

        order_by = self._args_to_order_by(order_by)
        args = self._args_to_clause(kwargs, clauses=_clauses)
        query = self.table.select(whereclause=args, limit=_limit, offset=_offset)
        if len(order_by):
            query = query.order_by(*order_by)

        conn = self.db.executable
        if _streamed:
            conn = self.db.engine.connect()
            conn = conn.execution_options(stream_results=True)

        return ResultIter(conn.execute(query), row_type=self.db.row_type, step=_step)

    def find_one(self, *args, **kwargs):
        """Get a single result from the table.

        Works just like :py:meth:`find() <dataset.Table.find>` but returns one
        result, or ``None``.
        ::

            row = table.find_one(country='United States')
        """
        if not self.exists:
            return None

        kwargs["_limit"] = 1
        kwargs["_step"] = None
        resiter = self.find(*args, **kwargs)
        try:
            for row in resiter:
                return row
        finally:
            resiter.close()

    def count(self, *_clauses, **kwargs):
        """Return the count of results for the given filter set."""
        # NOTE: this does not have support for limit and offset since I can't
        # see how this is useful. Still, there might be compatibility issues
        # with people using these flags. Let's see how it goes.
        if not self.exists:
            return 0

        args = self._args_to_clause(kwargs, clauses=_clauses)
        query = select([func.count()], whereclause=args)
        query = query.select_from(self.table)
        rp = self.db.executable.execute(query)
        return rp.fetchone()[0]

    def __len__(self):
        """Return the number of rows in the table."""
        return self.count()

    def distinct(self, *args, **_filter):
        """Return all the unique (distinct) values for the given ``columns``.
        ::

            # returns only one row per year, ignoring the rest
            table.distinct('year')
            # works with multiple columns, too
            table.distinct('year', 'country')
            # you can also combine this with a filter
            table.distinct('year', country='China')
        """
        if not self.exists:
            return iter([])

        columns = []
        clauses = []
        for column in args:
            if isinstance(column, ClauseElement):
                clauses.append(column)
            else:
                if not self.has_column(column):
                    raise DatasetException("No such column: %s" % column)
                columns.append(self.table.c[column])

        clause = self._args_to_clause(_filter, clauses=clauses)
        if not len(columns):
            return iter([])

        q = expression.select(
            columns,
            distinct=True,
            whereclause=clause,
            order_by=[c.asc() for c in columns],
        )
        return self.db.query(q)

    # Legacy methods for running find queries.
    all = find

    def __iter__(self):
        """Return all rows of the table as simple dictionaries.

        Allows for iterating over all rows in the table without explicetly
        calling :py:meth:`find() <dataset.Table.find>`.
        ::

            for row in table:
                print(row)
        """
        return self.find()

    def __repr__(self):
        """Get table representation."""
        return "<Table(%s)>" % self.table.name
