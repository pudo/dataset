import logging
from hashlib import sha1

from sqlalchemy.sql import and_, expression
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.schema import Column, Index
from sqlalchemy import alias, func
from dataset.persistence.util import guess_type, normalize_column_name
from dataset.persistence.util import ResultIter
from dataset.util import DatasetException


log = logging.getLogger(__name__)


class Table(object):
    """Represents a table in a database and exposes common operations."""

    def __init__(self, database, table):
        """Initialise the table from database schema."""
        self.indexes = dict((i.name, i) for i in table.indexes)
        self.database = database
        self.table = table
        self._is_dropped = False

    @property
    def columns(self):
        """Get a listing of all columns that exist in the table."""
        return list(self.table.columns.keys())

    @property
    def _normalized_columns(self):
        return map(normalize_column_name, self.columns)

    def drop(self):
        """
        Drop the table from the database.

        Delete both the schema and all the contents within it.
        Note: the object will raise an Exception if you use it after
        dropping the table. If you want to re-create the table, make
        sure to get a fresh instance from the :py:class:`Database <dataset.Database>`.
        """
        self.database._acquire()
        self._is_dropped = True
        self.database._tables.pop(self.table.name, None)
        self.table.drop(self.database.engine)
        self.database._release()
        return True

    def _check_dropped(self):
        if self._is_dropped:
            raise DatasetException('the table has been dropped. this object should not be used again.')

    def insert(self, row, ensure=None, types={}):
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
        res = self.database.executable.execute(self.table.insert(row))
        if len(res.inserted_primary_key) > 0:
            return res.inserted_primary_key[0]

    def insert_ignore(self, row, keys, ensure=None, types={}):
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
        res = self._upsert_pre_check(row, keys, ensure)
        if res is None:
            return self.insert(row, ensure=ensure, types=types)
        else:
            return False

    def insert_many(self, rows, chunk_size=1000, ensure=None, types={}):
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

    def update(self, row, keys, ensure=None, types={}):
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
        # check whether keys arg is a string and format as a list
        if not isinstance(keys, (list, tuple)):
            keys = [keys]
        self._check_dropped()
        if not keys or len(keys) == len(row):
            return False
        clause = [(u, row.get(u)) for u in keys]

        ensure = self.database.ensure_schema if ensure is None else ensure
        if ensure:
            self._ensure_columns(row, types=types)

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
        try:
            if not isinstance(keys, (list, tuple)):
                keys = [keys]
            self._check_dropped()

            ensure = self.database.ensure_schema if ensure is None else ensure
            if ensure:
                self.create_index(keys)

            filters = {}
            for key in keys:
                filters[key] = row.get(key)

            res = self.find_one(**filters)
        except:
            res = None

        return res

    def upsert(self, row, keys, ensure=None, types={}):
        """
        An UPSERT is a smart combination of insert and update.

        If rows with matching ``keys`` exist they will be updated, otherwise a
        new row is inserted in the table.
        ::

            data = dict(id=10, title='I am a banana!')
            table.upsert(data, ['id'])
        """
        res = self._upsert_pre_check(row, keys, ensure)
        if res is None:
            return self.insert(row, ensure=ensure, types=types)
        else:
            row_count = self.update(row, keys, ensure=ensure, types=types)
            try:
                result = (row_count > 0, res['id'])[row_count == 1]
            except KeyError:
                result = row_count > 0

            return result

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
        if _filter:
            q = self._args_to_clause(_filter, clauses=_clauses)
            stmt = self.table.delete(q)
        else:
            stmt = self.table.delete()
        rows = self.database.executable.execute(stmt)
        return rows.rowcount > 0

    def _has_column(self, column):
        return normalize_column_name(column) in self._normalized_columns

    def _ensure_columns(self, row, types={}):
        # Keep order of inserted columns
        for column in row.keys():
            if self._has_column(column):
                continue
            if column in types:
                _type = types[column]
            else:
                _type = guess_type(row[column])
            log.debug("Creating column: %s (%s) on %r" % (column,
                                                          _type, self.table.name))
            self.create_column(column, _type)

    def _args_to_clause(self, args, ensure=None, clauses=()):
        ensure = self.database.ensure_schema if ensure is None else ensure
        if ensure:
            self._ensure_columns(args)
        clauses = list(clauses)
        for k, v in args.items():
            if not self._has_column(k):
                clauses.append(func.sum(1) == 2)
            elif isinstance(v, (list, tuple)):
                clauses.append(self.table.c[k].in_(v))
            else:
                clauses.append(self.table.c[k] == v)
        return and_(*clauses)

    def create_column(self, name, type):
        """
        Explicitely create a new column ``name`` of a specified type.

        ``type`` must be a `SQLAlchemy column type <http://docs.sqlalchemy.org/en/rel_0_8/core/types.html>`_.
        ::

            table.create_column('created_at', sqlalchemy.DateTime)
        """
        self._check_dropped()
        self.database._acquire()

        try:
            if normalize_column_name(name) not in self._normalized_columns:
                self.database.op.add_column(
                    self.table.name,
                    Column(name, type),
                    self.table.schema
                )
                self.table = self.database.update_table(self.table.name)
        finally:
            self.database._release()

    def drop_column(self, name):
        """
        Drop the column ``name``.

        ::

            table.drop_column('created_at')
        """
        if self.database.engine.dialect.name == 'sqlite':
            raise NotImplementedError("SQLite does not support dropping columns.")
        self._check_dropped()
        self.database._acquire()
        try:
            if name in self.table.columns.keys():
                self.database.op.drop_column(
                    self.table.name,
                    name
                )
                self.table = self.database.update_table(self.table.name)
        finally:
            self.database._release()

    def create_index(self, columns, name=None):
        """
        Create an index to speed up queries on a table.

        If no ``name`` is given a random name is created.
        ::

            table.create_index(['name', 'country'])
        """
        self._check_dropped()
        if not name:
            sig = '||'.join(columns)

            # This is a work-around for a bug in <=0.6.1 which would create
            # indexes based on hash() rather than a proper hash.
            key = abs(hash(sig))
            name = 'ix_%s_%s' % (self.table.name, key)
            if name in self.indexes:
                return self.indexes[name]

            key = sha1(sig.encode('utf-8')).hexdigest()[:16]
            name = 'ix_%s_%s' % (self.table.name, key)

        if name in self.indexes:
            return self.indexes[name]
        try:
            self.database._acquire()
            columns = [self.table.c[c] for c in columns]
            idx = Index(name, *columns)
            idx.create(self.database.engine)
        except:
            idx = None
        finally:
            self.database._release()
        self.indexes[name] = idx
        return idx

    def find_one(self, *args, **kwargs):
        """
        Get a single result from the table.

        Works just like :py:meth:`find() <dataset.Table.find>` but returns one result, or None.

        ::

            row = table.find_one(country='United States')
        """
        kwargs['_limit'] = 1
        iterator = self.find(*args, **kwargs)
        try:
            return next(iterator)
        except StopIteration:
            return None

    def _args_to_order_by(self, order_by):
        if order_by[0] == '-':
            return self.table.c[order_by[1:]].desc()
        else:
            return self.table.c[order_by].asc()

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
        order_by = kwargs.pop('order_by', 'id')
        return_count = kwargs.pop('return_count', False)
        return_query = kwargs.pop('return_query', False)
        _filter = kwargs

        self._check_dropped()
        if not isinstance(order_by, (list, tuple)):
            order_by = [order_by]
        order_by = [o for o in order_by if (o.startswith('-') and o[1:] or o) in self.table.columns]
        order_by = [self._args_to_order_by(o) for o in order_by]

        args = self._args_to_clause(_filter, ensure=False, clauses=_clauses)

        # query total number of rows first
        count_query = alias(self.table.select(whereclause=args, limit=_limit, offset=_offset),
                            name='count_query_alias').count()
        rp = self.database.executable.execute(count_query)
        total_row_count = rp.fetchone()[0]
        if return_count:
            return total_row_count

        if _limit is None:
            _limit = total_row_count

        if _step is None or _step is False or _step == 0:
            _step = total_row_count

        query = self.table.select(whereclause=args, limit=_limit,
                                  offset=_offset, order_by=order_by)
        if return_query:
            return query
        return ResultIter(self.database.executable.execute(query),
                          row_type=self.database.row_type, step=_step)

    def count(self, *args, **kwargs):
        """Return the count of results for the given filter set."""
        return self.find(*args, return_count=True, **kwargs)

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
