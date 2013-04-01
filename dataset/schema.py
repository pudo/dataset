import logging
from datetime import datetime
from collections import defaultdict

from sqlalchemy import Integer, UnicodeText, Float, DateTime, Boolean
from sqlalchemy.schema import Table, MetaData, Column, Index
from sqlalchemy.sql import and_, expression

log = logging.getLogger(__name__)






def _ensure_columns(engine, table, row, types={}):
    columns = set(row.keys()) - set(table.columns.keys())
    for column in columns:
        if column in types:
            _type = types[column]
        else:
            _type = _guess_type(row[column])
        log.debug("Creating column: %s (%s) on %r" % (column, 
            _type, table.name))
        create_column(engine, table, column, _type)

def _args_to_clause(table, args):
    clauses = []
    for k, v in args.items():
        clauses.append(table.c[k] == v)
    return and_(*clauses)

def create_column(engine, table, name, type):
    table = get_table(engine, table)    
    with lock:
        if name not in table.columns.keys():
            col = Column(name, type)
            col.create(table, connection=engine)

def create_index(engine, table, columns, name=None):
    table = get_table(engine, table)    
    with lock:
        if not name:
            sig = abs(hash('||'.join(columns)))
            name = 'ix_%s_%s' % (table.name, sig)
        if name in engine._indexes:
            return engine._indexes[name]
        try:
            columns = [table.c[c] for c in columns]
            idx = Index(name, *columns)
            idx.create(engine)
        except:
            idx = None
        engine._indexes[name] = idx
        return idx

