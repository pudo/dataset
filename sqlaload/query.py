import logging

from sqlalchemy.sql import expression

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

def distinct(engine, table, *columns):
    columns = [table.c[c] for c in columns]
    q = expression.select(columns, distinct=True)
    return resultiter(engine.execute(q))

def all(engine, table):
    q = table.select()
    return resultiter(engine.execute(q))




