import logging

from sqlalchemy.sql import and_, expression
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.schema import Column, Index
from sqlalchemy import func, select, false
from sqlalchemy.engine.reflection import Inspector

from dataset.persistence.util import normalize_column_name, index_name
from dataset.persistence.util import ResultIter
from dataset.util import DatasetException


log = logging.getLogger(__name__)


class Table(object):
    """Represents a table in a database and exposes common operations."""
    PRIMARY_DEFAULT = 'id'

    def __init__(self, database, table):
        """Initialise the table from database schema."""
        self.database = database
        self.name = table.name
        self.table = table
        self._is_dropped = False
        self._indexes = []

    @property
    def exists(self):
        """Check to see if the table currently exists in the database."""
        if self.table is not None:
            return True
        return self.name in self.database

    @property
    def columns(self):
        """Get a listing of all columns that exist in the table."""
        if not self.exists:
            return []
        return self.table.columns.keys()

    def drop(self):
        """
        Drop the table from the database.

        Delete both the schema and all the contents within it.
        Note: the object will raise an Exception if you use it after
        dropping the table. If you want to re-create the table, make
        sure to get a fresh instance from the :py:class:`Database <dataset.Database>`.
        """
        self._check_dropped()
        with self.database.lock:
            self.table.drop(self.database.executable, checkfirst=True)
            # self.database._tables.pop(self.name, None)
            self.table = None

    def _check_dropped(self):
        # self.table = self.database._reflect_table(self.name)
        if self.table is None:
            raise DatasetException('The table has been dropped.')

    def insert(self, row, ensure=None, types=None):
        """
        Add a row (type: dict) by inserting it into the table.

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
        self._check_dropped()
        ensure = self.database.ensure_schema if ensure is None else ensure
        if ensure:
            self._ensure_columns(row, types=types)
        else:
            row = self._prune_row(row)
        res = self.database.executable.execute(self.table.insert(row))
        if len(res.inserted_primary_key) > 0:
            return res.inserted_primary_key[0]

    def insert_ignore(self, row, keys, ensure=None, types=None):
        """
        Add a row (type: dict) into the table if the row does not exist.

        If rows with matching ``keys`` exist they will be added to the table.

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
        row, res = self._upsert_pre_check(row, keys, ensure)
        if res is None:
            return self.insert(row, ensure=ensure, types=types)
        else:
            return False

    def insert_many(self, rows, chunk_size=1000, ensure=None, types=None):
        """
        Add many rows at a time.

        This is significantly faster than adding them one by one. Per default
        the rows are processed in chunks of 1000 per commit, unless you specify
        a different ``chunk_size``.

        See :py:meth:`insert() <dataset.Table.insert>` for details on
        the other parameters.
        ::

            rows = [dict(name='Dolly')] * 10000
            table.insert_many(rows)
        """
        ensure = self.database.ensure_schema if ensure is None else ensure

        def _process_chunk(chunk):
            if ensure:
                for row in chunk:
                    self._ensure_columns(row, types=types)
            else:
                chunk = [self._prune_row(r) for r in chunk]
            self.table.insert().execute(chunk)
        self._check_dropped()

        chunk = []
        for i, row in enumerate(rows, start=1):
            chunk.append(row)
            if i % chunk_size == 0:
                _process_chunk(chunk)
                chunk = []

        if chunk:
            _process_chunk(chunk)

    def update(self, row, keys, ensure=None, types=None):
        """
        Update a row in the table.

        The update is managed via the set of column names stated in ``keys``:
        they will be used as filters for the data to be updated, using the values
        in ``row``.
        ::

            # update all entries with id matching 10, setting their title columns
            data = dict(id=10, title='I am a banana!')
            table.update(data, ['id'])

        If keys in ``row`` update columns not present in the table,
        they will be created based on the settings of ``ensure`` and
        ``types``, matching the behavior of :py:meth:`insert() <dataset.Table.insert>`.
        """
        self._check_dropped()
        # check whether keys arg is a string and format as a list
        if not isinstance(keys, (list, tuple)):
            keys = [keys]
        if not keys or len(keys) == len(row):
            return False
        clause = [(u, row.get(u)) for u in keys]

        ensure = self.database.ensure_schema if ensure is None else ensure
        if ensure:
            self._ensure_columns(row, types=types)
        else:
            row = self._prune_row(row)

        # Don't update the key itself, so remove any keys from the row dict
        clean_row = row.copy()
        for key in keys:
            if key in clean_row.keys():
                del clean_row[key]

        try:
            filters = self._args_to_clause(dict(clause))
            stmt = self.table.update(filters, clean_row)
            rp = self.database.executable.execute(stmt)
            return rp.rowcount
        except KeyError:
            return 0

    def _upsert_pre_check(self, row, keys, ensure):
        # check whether keys arg is a string and format as a list
        if not isinstance(keys, (list, tuple)):
            keys = [keys]
        self._check_dropped()

        ensure = self.database.ensure_schema if ensure is None else ensure
        if ensure:
            self.create_index(keys)
        else:
            row = self._prune_row(row)

        filters = {}
        for key in keys:
            filters[key] = row.get(key)
        return row, self.find_one(**filters)

    def upsert(self, row, keys, ensure=None, types=None):
        """
        An UPSERT is a smart combination of insert and update.

        If rows with matching ``keys`` exist they will be updated, otherwise a
        new row is inserted in the table.
        ::

            data = dict(id=10, title='I am a banana!')
            table.upsert(data, ['id'])
        """
        row, res = self._upsert_pre_check(row, keys, ensure)
        if res is None:
            return self.insert(row, ensure=ensure, types=types)
        else:
            row_count = self.update(row, keys, ensure=ensure, types=types)
            try:
                result = (row_count > 0, res['id'])[row_count == 1]
            except KeyError:
                result = row_count > 0

            return result

    def upsert_many(self, rows, keys, ensure=None, types={},chunk_size=1000):
        """
        Sorts multiple input rows into upserts and inserts. Inserts are passed to insert_many and upserts are updated.

        See :py:meth:`upsert() <dataset.Table.upsert>` and :py:meth:`insert_many() <dataset.Table.insert_many>`.

        """

        upserts = [self.find_one(**{key:row.get(key) for key in keys}) is not None for row in rows]
        upserts,inserts=[row for upsert,row in zip(upserts,rows) if upsert],[row for upsert,row in zip(upserts,rows) if not upsert]
        upsert_count=0
        for row in upserts:
            upsert_count+=self.update(row, keys, ensure=ensure, types=types)

        self.insert_many(inserts,chunk_size=chunk_size, ensure=ensure, types=types)




    def delete(self, *_clauses, **_filter):
        """

        Delete rows from the table.

        Keyword arguments can be used to add column-based filters. The filter
        criterion will always be equality:

        .. code-block:: python

            table.delete(place='Berlin')

        If no arguments are given, all records are deleted.
        """
        self._check_dropped()
        if not self.exists:
            return
        if _filter or _clauses:
            q = self._args_to_clause(_filter, clauses=_clauses)
            stmt = self.table.delete(q)
        else:
            stmt = self.table.delete()
        rows = self.database.executable.execute(stmt)
        return rows.rowcount > 0

    def _has_column(self, column):
        return normalize_column_name(column) in self.columns

    def _ensure_columns(self, row, types=None):
        # Keep order of inserted columns
        for column in row.keys():
            if self._has_column(column):
                continue
            if types is not None and column in types:
                _type = types[column]
            else:
                _type = self.database.types.guess(row[column])
            log.debug("Creating column: %s (%s) on %r" % (column,
                                                          _type,
                                                          self.table.name))
            self.create_column(column, _type)

    def _prune_row(self, row):
        """Remove keys from row not in column set."""
        # normalize keys
        row = {normalize_column_name(k): v for k, v in row.items()}
        # filter out keys not in column set
        return {k: row[k] for k in row if k in self.columns}

    def _args_to_clause(self, args, ensure=None, clauses=()):
        ensure = self.database.ensure_schema if ensure is None else ensure
        if ensure:
            self._ensure_columns(args)
        clauses = list(clauses)
        for k, v in args.items():
            if not self._has_column(k):
                clauses.append(false())
            elif isinstance(v, (list, tuple)):
                clauses.append(self.table.c[k].in_(v))
            else:
                clauses.append(self.table.c[k] == v)
        return and_(*clauses)

    def create_column(self, name, type):
        """
        Explicitly create a new column ``name`` of a specified type.

        ``type`` must be a `SQLAlchemy column type <http://docs.sqlalchemy.org/en/rel_0_8/core/types.html>`_.
        ::

            table.create_column('created_at', db.types.datetime)
        """
        self._check_dropped()
        with self.database.lock:
            name = normalize_column_name(name)
            if name in self.columns:
                log.debug("Column exists: %s" % name)
                return

            self.database.op.add_column(
                self.table.name,
                Column(name, type),
                self.table.schema
            )
            self.table = self.database._reflect_table(self.table.name)

    def create_column_by_example(self, name, value):
        """
        Explicitly create a new column ``name`` with a type that is appropriate to store
        the given example ``value``.  The type is guessed in the same way as for the
        insert method with ``ensure=True``. If a column of the same name already exists,
        no action is taken, even if it is not of the type we would have created.

            table.create_column_by_example('length', 4.2)
        """
        type_ = self.database.types.guess(value)
        self.create_column(name, type_)

    def drop_column(self, name):
        """Drop the column ``name``.

        ::
            table.drop_column('created_at')
        """
        if self.database.engine.dialect.name == 'sqlite':
            raise NotImplementedError("SQLite does not support dropping columns.")
        if not self.exists:
            return
        self._check_dropped()
        if name not in self.columns:
            log.debug("Column does not exist: %s", name)
            return
        with self.database.lock:
            self.database.op.drop_column(
                self.table.name,
                name,
                self.table.schema
            )
            self.table = self.database._reflect_table(self.table.name)

    def has_index(self, columns):
        """Check if an index exists to cover the given `columns`."""
        columns = set([normalize_column_name(c) for c in columns])
        if columns in self._indexes:
            return True
        inspector = Inspector.from_engine(self.database.executable)
        indexes = inspector.get_indexes(self.name, schema=self.database.schema)
        for index in indexes:
            if columns == set(index.get('column_names', [])):
                self._indexes.append(columns)
                return True
        return False

    def create_index(self, columns, name=None, **kw):
        """
        Create an index to speed up queries on a table.

        If no ``name`` is given a random name is created.
        ::

            table.create_index(['name', 'country'])
        """
        self._check_dropped()
        columns = [normalize_column_name(c) for c in columns]
        with self.database.lock:
            if not self.has_index(columns):
                name = name or index_name(self.name, columns)
                columns = [self.table.c[c] for c in columns]
                idx = Index(name, *columns, **kw)
                idx.create(self.database.executable)

    def _args_to_order_by(self, order_by):
        if not isinstance(order_by, (list, tuple)):
            order_by = [order_by]
        orderings = []
        for ordering in order_by:
            if ordering is None:
                continue
            column = ordering.lstrip('-')
            if column not in self.table.columns:
                continue
            if ordering.startswith('-'):
                orderings.append(self.table.c[column].desc())
            else:
                orderings.append(self.table.c[column].asc())
        return orderings

    def find(self, *_clauses, **kwargs):
        """
        Perform a simple search on the table.

        Simply pass keyword arguments as ``filter``.
        ::

            results = table.find(country='France')
            results = table.find(country='France', year=1980)

        Using ``_limit``::

            # just return the first 10 rows
            results = table.find(country='France', _limit=10)

        You can sort the results by single or multiple columns. Append a minus sign
        to the column name for descending order::

            # sort results by a column 'year'
            results = table.find(country='France', order_by='year')
            # return all rows sorted by multiple columns (by year in descending order)
            results = table.find(order_by=['country', '-year'])

        For more complex queries, please use :py:meth:`db.query() <dataset.Database.query>`
        instead.
        """
        _limit = kwargs.pop('_limit', None)
        _offset = kwargs.pop('_offset', 0)
        _step = kwargs.pop('_step', 5000)
        order_by = kwargs.pop('order_by', None)

        self._check_dropped()
        order_by = self._args_to_order_by(order_by)
        args = self._args_to_clause(kwargs, ensure=False, clauses=_clauses)

        if _step is False or _step == 0:
            _step = None

        query = self.table.select(whereclause=args, limit=_limit,
                                  offset=_offset)
        if len(order_by):
            query = query.order_by(*order_by)
        return ResultIter(self.database.executable.execute(query),
                          row_type=self.database.row_type, step=_step)

    def find_one(self, *args, **kwargs):
        """Get a single result from the table.

        Works just like :py:meth:`find() <dataset.Table.find>` but returns one
        result, or None.
        ::

            row = table.find_one(country='United States')
        """
        if not self.exists:
            return None
        kwargs['_limit'] = 1
        kwargs['_step'] = None
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
        self._check_dropped()
        args = self._args_to_clause(kwargs, ensure=False, clauses=_clauses)
        query = select([func.count()], whereclause=args)
        query = query.select_from(self.table)
        rp = self.database.executable.execute(query)
        return rp.fetchone()[0]

    def __len__(self):
        """Return the number of rows in the table."""
        return self.count()

    def distinct(self, *args, **_filter):
        """
        Return all rows of a table, but remove rows in with duplicate values in ``columns``.

        Interally this creates a `DISTINCT statement <http://www.w3schools.com/sql/sql_distinct.asp>`_.
        ::

            # returns only one row per year, ignoring the rest
            table.distinct('year')
            # works with multiple columns, too
            table.distinct('year', 'country')
            # you can also combine this with a filter
            table.distinct('year', country='China')
        """
        self._check_dropped()
        qargs = []
        columns = []
        try:
            for c in args:
                if isinstance(c, ClauseElement):
                    qargs.append(c)
                else:
                    columns.append(self.table.c[c])
            for col, val in _filter.items():
                qargs.append(self.table.c[col] == val)
        except KeyError:
            return []

        q = expression.select(columns, distinct=True,
                              whereclause=and_(*qargs),
                              order_by=[c.asc() for c in columns])
        return self.database.query(q)

    def __getitem__(self, item):
        """
        Get distinct column values.

        This is an alias for distinct which allows the table to be queried as using
        square bracket syntax.
        ::
            # Same as distinct:
            print list(table['year'])
        """
        if not isinstance(item, tuple):
            item = item,
        return self.distinct(*item)

    def all(self):
        """
        Return all rows of the table as simple dictionaries.

        This is simply a shortcut to *find()* called with no arguments.
        ::

            rows = table.all()
        """
        return self.find()

    def __iter__(self):
        """
        Return all rows of the table as simple dictionaries.

        Allows for iterating over all rows in the table without explicetly
        calling :py:meth:`all() <dataset.Table.all>`.
        ::

            for row in table:
                print(row)
        """
        return self.all()

    def __repr__(self):
        """Get table representation."""
        return '<Table(%s)>' % self.table.name
