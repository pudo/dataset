import logging
from itertools import count

from sqlalchemy.sql import and_, expression
from sqlalchemy.schema import Column, Index

from dataset.persistence.util import guess_type
from dataset.persistence.util import ResultIter
from dataset.util import DatasetException


log = logging.getLogger(__name__)


class Table(object):

    def __init__(self, database, table):
        self.indexes = dict((i.name, i) for i in table.indexes)
        self.database = database
        self.table = table
        self._is_dropped = False

    @property
    def columns(self):
        """
        Get a listing of all columns that exist in the table.

        >>> print 'age' in table.columns
        True
        """
        return set(self.table.columns.keys())

    def drop(self):
        """
        Drop the table from the database, deleting both the schema
        and all the contents within it.

        Note: the object will raise an Exception if you use it after
        dropping the table. If you want to re-create the table, make
        sure to get a fresh instance from the :py:class:`Database <dataset.Database>`.
        """
        self.database._acquire()
        self._is_dropped = True
        self.database._tables.pop(self.table.name, None)
        self.table.drop(self.database.engine)

    def _check_dropped(self):
        if self._is_dropped:
            raise DatasetException('the table has been dropped. this object should not be used again.')

    def insert(self, row, ensure=True, types={}):
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
        """
        self._check_dropped()
        if ensure:
            self._ensure_columns(row, types=types)
        res = self.database.executable.execute(self.table.insert(row))
        return res.inserted_primary_key[0]

    def insert_many(self, rows, chunk_size=1000, ensure=True, types={}):
        """
        Add many rows at a time, which is significantly faster than adding
        them one by one. Per default the rows are processed in chunks of
        1000 per commit, unless you specify a different ``chunk_size``.

        See :py:meth:`insert() <dataset.Table.insert>` for details on
        the other parameters.
        ::

            rows = [dict(name='Dolly')] * 10000
            table.insert_many(rows)
        """
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
        

    def update(self, row, keys, ensure=True, types={}):
        """
        Update a row in the table. The update is managed via
        the set of column names stated in ``keys``: they will be
        used as filters for the data to be updated, using the values
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
        if isinstance(keys, basestring):
            keys = [keys]
            
        self._check_dropped()
        if not keys or len(keys)==len(row):
            return False
        clause = [(u, row.get(u)) for u in keys]
        """
        Don't update the key itself, so remove any keys from the row dict
        """
        for key in keys:
            if key in row.keys():
                del row[key]

        if ensure:
            self._ensure_columns(row, types=types)
        try:
            filters = self._args_to_clause(dict(clause))
            stmt = self.table.update(filters, row)
            rp = self.database.executable.execute(stmt)
            return rp.rowcount > 0
        except KeyError:
            return False

    def upsert(self, row, keys, ensure=True, types={}):
        """
        An UPSERT is a smart combination of insert and update. If rows with matching ``keys`` exist
        they will be updated, otherwise a new row is inserted in the table.
        ::

            data = dict(id=10, title='I am a banana!')
            table.upsert(data, ['id'])
        """
        # check whether keys arg is a string and format as a list
        if isinstance(keys, basestring):
            keys = [keys]
            
        self._check_dropped()
        if ensure:
            self.create_index(keys)

        filters = {}
        for key in keys:
            filters[key] = row.get(key)

        if self.find_one(**filters) is not None:
            self.update(row, keys, ensure=ensure, types=types)
        else:
            self.insert(row, ensure=ensure, types=types)

    def delete(self, **_filter):
        """ Delete rows from the table. Keyword arguments can be used
        to add column-based filters. The filter criterion will always
        be equality:

        .. code-block:: python

            table.delete(place='Berlin')

        If no arguments are given, all records are deleted.
        """
        self._check_dropped()
        if _filter:
            q = self._args_to_clause(_filter)
            stmt = self.table.delete(q)
        else:
            stmt = self.table.delete()
        self.database.executable.execute(stmt)

    def _ensure_columns(self, row, types={}):
        for column in set(row.keys()) - set(self.table.columns.keys()):
            if column in types:
                _type = types[column]
            else:
                _type = guess_type(row[column])
            log.debug("Creating column: %s (%s) on %r" % (column,
                                                          _type, self.table.name))
            self.create_column(column, _type)

    def _args_to_clause(self, args):
        self._ensure_columns(args)
        clauses = []
        for k, v in args.items():
            if isinstance(v, (list, tuple)):
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
            if name not in self.table.columns.keys():
                col = Column(name, type)
                col.create(self.table,
                           connection=self.database.executable)
        finally:
            self.database._release()

    def create_index(self, columns, name=None):
        """
        Create an index to speed up queries on a table. If no ``name`` is given a random name is created.
        ::

            table.create_index(['name', 'country'])
        """
        self._check_dropped()
        if not name:
            sig = abs(hash('||'.join(columns)))
            name = 'ix_%s_%s' % (self.table.name, sig)
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

    def find_one(self, **_filter):
        """
        Works just like :py:meth:`find() <dataset.Table.find>` but returns one result, or None.
        ::

            row = table.find_one(country='United States')
        """
        self._check_dropped()
        args = self._args_to_clause(_filter)
        query = self.table.select(whereclause=args, limit=1)
        rp = self.database.executable.execute(query)
        data = rp.fetchone()
        if data is not None:
            return dict(zip(rp.keys(), data))

    def _args_to_order_by(self, order_by):
        if order_by[0] == '-':
            return self.table.c[order_by[1:]].desc()
        else:
            return self.table.c[order_by].asc()

    def find(self, _limit=None, _offset=0, _step=5000,
             order_by='id', **_filter):
        """
        Performs a simple search on the table. Simply pass keyword arguments as ``filter``.
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

        By default :py:meth:`find() <dataset.Table.find>` will break the
        query into chunks of ``_step`` rows to prevent huge tables
        from being loaded into memory at once.

        For more complex queries, please use :py:meth:`db.query() <dataset.Database.query>`
        instead."""
        self._check_dropped()
        if isinstance(order_by, (str, unicode)):
            order_by = [order_by]
        order_by = filter(lambda o: o in self.table.columns, order_by)
        order_by = [self._args_to_order_by(o) for o in order_by]

        args = self._args_to_clause(_filter)

        # query total number of rows first
        count_query = self.table.count(whereclause=args, limit=_limit, offset=_offset)
        rp = self.database.executable.execute(count_query)
        total_row_count = rp.fetchone()[0]

        if _step is None or _step is False or _step == 0:
            _step = total_row_count

        if total_row_count > _step and not order_by:
            _step = total_row_count
            log.warn("query cannot be broken into smaller sections because it is unordered")

        queries = []

        for i in count():
            qoffset = _offset + (_step * i)
            qlimit = _step
            if _limit is not None:
                qlimit = min(_limit - (_step * i), _step)
            if qlimit <= 0:
                break
            if qoffset > total_row_count:
                break
            queries.append(self.table.select(whereclause=args, limit=qlimit,
                                             offset=qoffset, order_by=order_by))
        return ResultIter((self.database.executable.execute(q) for q in queries))

    def __len__(self):
        """
        Returns the number of rows in the table.
        """
        d = self.database.query(self.table.count()).next()
        return d.values().pop()

    def distinct(self, *columns, **_filter):
        """
        Returns all rows of a table, but removes rows in with duplicate values in ``columns``.
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
        try:
            columns = [self.table.c[c] for c in columns]
            for col, val in _filter.items():
                qargs.append(self.table.c[col] == val)
        except KeyError:
            return []

        q = expression.select(columns, distinct=True,
                              whereclause=and_(*qargs),
                              order_by=[c.asc() for c in columns])
        return self.database.query(q)

    def all(self):
        """
        Returns all rows of the table as simple dictionaries. This is simply a shortcut
        to *find()* called with no arguments.
        ::

            rows = table.all()"""
        return self.find()

    def __iter__(self):
        """
        Allows for iterating over all rows in the table without explicetly
        calling :py:meth:`all() <dataset.Table.all>`.
        ::

            for row in table:
                print row
        """
        return self.all()
