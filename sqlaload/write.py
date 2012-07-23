import logging

from sqlaload.schema import _ensure_columns, _args_to_clause
from sqlaload.schema import create_index

log = logging.getLogger(__name__)

def add_row(engine, table, row, ensure=True, types={}):
    """ Add a row (type: dict). If ``ensure`` is set, any of 
    the keys of the row are not table columns, they will be type
    guessed and created. """
    if ensure:
        _ensure_columns(engine, table, row, types=types)
    engine.execute(table.insert(row))

def update_row(engine, table, row, unique, ensure=True, types={}):
    if not len(unique):
        return False
    clause = dict([(u, row.get(u)) for u in unique])
    if ensure:
        _ensure_columns(engine, table, row, types=types)
    try:
        stmt = table.update(_args_to_clause(table, clause), row)
        rp = engine.execute(stmt)
        return rp.rowcount > 0
    except KeyError, ke:
        log.warn("UPDATE: filter column does not exist: %s" % ke)
        return False

def upsert(engine, table, row, unique, ensure=True, types={}):
    if ensure:
        create_index(engine, table, unique)

    if not update_row(engine, table, row, unique, ensure=ensure, types=types):
        add_row(engine, table, row, ensure=ensure, types=types)

def update(engine, table, criteria, values, ensure=True, types={}):
    if ensure:
        _ensure_columns(engine, table, values, types=types)
    q = table.update().values(values)
    for column, value in criteria.items():
        q = q.where(table.c[column]==value)
    engine.execute(q)

def delete(engine, table, **kw):
    _ensure_columns(engine, table, kw)

    qargs = []
    try:
        for col, val in kw.items():
            qargs.append(table.c[col]==val)
    except KeyError:
        return

    q = table.delete()
    for k, v in kw.items():
        q= q.where(table.c[k]==v)
    engine.execute(q)

