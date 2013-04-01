import logging
from datetime import datetime
from collections import defaultdict
from threading import RLock

from sqlalchemy import create_engine
from sqlalchemy import Integer, UnicodeText, Float, DateTime, Boolean
from sqlalchemy.schema import Table, MetaData, Column, Index
from sqlalchemy.sql import and_, expression
from migrate.versioning.util import construct_engine

log = logging.getLogger(__name__)
lock = RLock()

def connect(url):
    """ Create an engine for the given database URL. """
    kw = {}
    if url.startswith('postgres'):
        #kw['pool_size'] = 5
        from sqlalchemy.pool import NullPool
        kw['poolclass'] = NullPool
    engine = create_engine(url, **kw)
    engine = construct_engine(engine)
    meta = MetaData()
    meta.bind = engine
    engine._metadata = meta
    engine._tables = dict()
    engine._indexes = dict()
    return engine

def create_table(engine, table_name):
    with lock:
        log.debug("Creating table: %s on %r" % (table_name, engine))
        table = Table(table_name, engine._metadata)
        col = Column('id', Integer, primary_key=True)
        table.append_column(col)
        table.create(engine)
        engine._tables[table_name] = table
        return table

def load_table(engine, table_name):
    with lock:
        log.debug("Loading table: %s on %r" % (table_name, engine))
        table = Table(table_name, engine._metadata, autoload=True)
        engine._tables[table_name] = table
        return table

def get_table(engine, table_name):
    if isinstance(table_name, Table):
        return table_name

    # Accept Connection objects here
    if hasattr(engine, 'engine'):
        engine = engine.engine
    
    with lock:
        if table_name in engine._tables:
            return engine._tables[table_name]
        if engine.has_table(table_name):
            return load_table(engine, table_name)
        else:
            return create_table(engine, table_name)

def drop_table(engine, table_name):
    # Accept Connection objects here
    if hasattr(engine, 'engine'):
        engine = engine.engine

    with lock:
        if table_name in engine._tables:
            table = engine._tables[table_name]
        elif engine.has_table(table_name):
            table = Table(table_name, engine._metadata)
        else:
            return
        table.drop(engine)
        engine._tables.pop(table_name, None)

def _guess_type(sample):
    if isinstance(sample, bool):
        return Boolean
    elif isinstance(sample, int):
        return Integer
    elif isinstance(sample, float):
        return Float
    elif isinstance(sample, datetime):
        return DateTime
    return UnicodeText

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

