import logging
from itertools import count

from sqlalchemy.sql import and_, expression
from sqlalchemy.schema import Column, Index

from dataset.persistence.util import guess_type


log = logging.getLogger(__name__)


class Table(object):

    def __init__(self, database, table):
        self.indexes = {}
        self.database = database
        self.table = table

    def drop(self):
        """ Drop the table from the database, deleting both the schema 
        and all the contents within it.
        
        Note: the object will be in an unusable state after using this
        command and should not be used again. If you want to re-create
        the table, make sure to get a fresh instance from the
        :py:class:`dataset.Database`. """
        with self.database.lock:
            self.database.tables.pop(self.table.name, None)
            self.table.drop(engine)

    def insert(self, row, ensure=True, types={}):
        """ Add a row (type: dict) by inserting it into the database.
        If ``ensure`` is set, any of the keys of the row are not
        table columns, they will be created automatically. 
        
        During column creation, ``types`` will be checked for a key
        matching the name of a column to be created, and the given 
        SQLAlchemy column type will be used. Otherwise, the type is
        guessed from the row's value, defaulting to a simple unicode
        field. """
        if ensure:
            self._ensure_columns(row, types=types)
        self.database.engine.execute(self.table.insert(row))

    def update(self, row, unique, ensure=True, types={}):
        """ Update a row in the database. The update is managed via
        the set of column names stated in ``unique``: they will be 
        used as filters for the data to be updated, using the values
        in ``row``. Example:

        .. code-block:: python

            data = dict(id=10, title='I am a banana!')
            table.update(data, ['id'])

        This will update all entries matching the given ``id``, setting
        their ``title`` column.

        If keys in ``row`` update columns not present in the table, 
        they will be created based on the settings of ``ensure`` and 
        ``types``, matching the behaviour of ``insert``.
        """
        if not len(unique):
            return False
        clause = [(u, row.get(u)) for u in unique]
        if ensure:
            self._ensure_columns(row, types=types)
        try:
            filters = self._args_to_clause(dict(clause))
            stmt = self.table.update(filters, row)
            rp = self.database.engine.execute(stmt)
            return rp.rowcount > 0
        except KeyError, ke:
            return False

    def upsert(self, row, unique, ensure=True, types={}):
        if ensure:
            self.create_index(unique)

        if not self.update(row, unique, ensure=ensure, types=types):
            self.insert(row, ensure=ensure, types=types)

    def delete(self, **kw):
        q = self._args_to_clause(kw)
        stmt = self.table.delete(q)
        self.database.engine.execute(stmt)

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
            clauses.append(self.table.c[k] == v)
        return and_(*clauses)

    def create_column(self, name, type):
        with self.database.lock:
            if name not in self.table.columns.keys():
                col = Column(name, type)
                col.create(self.table,
                        connection=self.database.engine)

    def create_index(self, columns, name=None):
        with self.database.lock:
            if not name:
                sig = abs(hash('||'.join(columns)))
                name = 'ix_%s_%s' % (self.table.name, sig)
            if name in self.indexes:
                return self.indexes[name]
            try:
                columns = [self.table.c[c] for c in columns]
                idx = Index(name, *columns)
                idx.create(self.database.engine)
            except:
                idx = None
            self.indexes[name] = idx
            return idx

    def find_one(self, **kw):
        res = list(self.find(_limit=1, **kw))
        if not len(res):
            return None
        return res[0]

    def find(self, _limit=None, _step=5000, _offset=0,
             order_by='id', **kw):
        order_by = [self.table.c[order_by].asc()]
        args = self._args_to_clause(kw)

        for i in count():
            qoffset = _offset + (_step * i)
            qlimit = _step
            if _limit is not None:
                qlimit = min(_limit-(_step*i), _step)
            if qlimit <= 0:
                break
            q = self.table.select(whereclause=args, limit=qlimit,
                    offset=qoffset, order_by=order_by)
            rows = list(self.database.query(q))
            if not len(rows):
                return 
            for row in rows:
                yield row

    def __len__(self):
        d = self.database.query(self.table.count()).next()
        return d.values().pop()

    def distinct(self, *columns, **kw):
        qargs = []
        try:
            columns = [self.table.c[c] for c in columns]
            for col, val in kw.items():
                qargs.append(self.table.c[col]==val)
        except KeyError:
            return []

        q = expression.select(columns, distinct=True,
                whereclause=and_(*qargs),
                order_by=[c.asc() for c in columns])
        return self.database.query(q)

    def all(self):
        return self.find()

