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
        with self.database.lock:
            self.database.tables.pop(self.table.name, None)
            self.table.drop(engine)

    def insert(self, row, ensure=True, types={}):
        """ Add a row (type: dict). If ``ensure`` is set, any of 
        the keys of the row are not table columns, they will be type
        guessed and created. """
        if ensure:
            self._ensure_columns(row, types=types)
        self.database.engine.execute(self.table.insert(row))

    def update(self, row, unique, ensure=True, types={}):
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
        self.database.engine.execute(q)

    def _ensure_columns(self, row, types={}):
        for column in set(row.keys()) - set(self.columns.keys()):
            if column in types:
                _type = types[column]
            else:
                _type = guess_type(row[column])
            log.debug("Creating column: %s (%s) on %r" % (column, 
                _type, self.table.name))
            self.create_column(column, _type)

    def _args_to_clause(self, args):
        self._ensure_columns(kw)
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

    def find_one(**kw):
        res = list(self.find(self.database.engine,
            self.table, _limit=1, **kw))
        if not len(res):
            return None
        return res[0]

    def find(engine, _limit=None, _step=5000, _offset=0,
             order_by='id', **kw):
        order_by = [table.c[order_by].asc()]
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

