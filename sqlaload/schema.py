import logging
from datetime import datetime
from collections import defaultdict

from sqlalchemy import create_engine
from sqlalchemy import Integer, UnicodeText, Float, DateTime, Boolean
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.sql import and_, expression
from migrate.versioning.util import construct_engine

log = logging.getLogger(__name__)

TABLES = defaultdict(dict)

def connect(url):
    """ Create an engine for the given database URL. """
    engine = create_engine(url, pool_size=1)
    engine = construct_engine(engine)
    meta = MetaData()
    meta.bind = engine
    engine._metadata = meta
    return engine

def create_table(engine, table_name):
    log.debug("Creating table: %s on %r" % (table_name, engine))
    table = Table(table_name, engine._metadata)
    col = Column('id', Integer, primary_key=True)
    table.append_column(col)
    table.create(engine)
    TABLES[engine][table_name] = table
    return table

def load_table(engine, table_name):
    log.debug("Loading table: %s on %r" % (table_name, engine))
    table = Table(table_name, engine._metadata, autoload=True)
    TABLES[engine][table_name] = table
    return table

def get_table(engine, table_name):
    if table_name in TABLES[engine]:
        return TABLES[engine][table_name]
    if engine.has_table(table_name):
        return load_table(engine, table_name)
    else:
        return create_table(engine, table_name)

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
    if name not in table.columns.keys():
        col = Column(name, type)
        col.create(table, connection=engine)

