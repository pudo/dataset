import logging
from itertools import count

from sqlalchemy.sql import expression, and_
from sqlaload.schema import _ensure_columns

log = logging.getLogger(__name__)

def resultiter(rp):
    """ SQLAlchemy ResultProxies are not iterable to get a 
    list of dictionaries. This is to wrap them. """
    keys = rp.keys()
    while True:
        row = rp.fetchone()
        if row is None:
            break
        yield dict(zip(keys, row))

def find_one(engine, table, **kw):
    res = list(find(engine, table, _limit=1, **kw))
    if not len(res):
        return None
    return res[0]

def find(engine, table, _limit=None, _step=5000, _offset=0,
         order_by=None, **kw):
    _ensure_columns(engine, table, kw)

    if order_by is None:
        order_by = [table.c.id.asc()]
    else:
        order_by = [table.c[order_by].asc()]

    qargs = []
    try:
        for col, val in kw.items():
            qargs.append(table.c[col]==val)
    except KeyError:
        return

    for i in count():
        qoffset = _offset + (_step * i)
        qlimit = _step
        if _limit is not None:
            qlimit = min(_limit-(_step*i), _step)
        if qlimit <= 0:
            break
        q = table.select(whereclause=and_(*qargs), limit=qlimit,
                offset=qoffset, order_by=order_by)
        #print q
        rows = list(resultiter(engine.execute(q)))
        if not len(rows):
            return 
        for row in rows:
            yield row

def query(engine, query):
    for res in resultiter(engine.execute(query)):
        yield res

def distinct(engine, table, *columns, **kw):

    qargs = []
    try:
        columns = [table.c[c] for c in columns]
        for col, val in kw.items():
            qargs.append(table.c[col]==val)
    except KeyError:
        return []

    q = expression.select(columns, distinct=True,
            whereclause=and_(*qargs),
            order_by=[c.asc() for c in columns])
    return list(resultiter(engine.execute(q)))

def all(engine, table):
    return find(engine, table)




