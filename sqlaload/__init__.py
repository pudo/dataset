import logging
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import Integer, UnicodeText, Float, DateTime
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.sql import and_, expression
from migrate.versioning.util import construct_engine

log = logging.getLogger(__name__)

def connect(url):
    """ Create an engine for the given database URL. """
    engine = create_engine(url)
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
    return table

def load_table(engine, table_name):
    return Table(table_name, engine._metadata, autoload=True)

def get_table(engine, table_name):
    if engine.has_table(table_name):
        return load_table(engine, table_name)
    else:
        return create_table(engine, table_name)

def _guess_type(sample):
    if isinstance(sample, int):
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
        col = Column(column, _type)
        col.create(table, connection=engine)

def _args_to_clause(table, args):
    clauses = []
    for k, v in args.items():
        clauses.append(table.c[k] == v)
    return and_(*clauses)


def add_row(engine, table, row, ensure=False, types={}):
    """ Add a row (type: dict). If ``ensure`` is set, any of 
    the keys of the row are not table columns, they will be type
    guessed and created. """
    if ensure:
        _ensure_columns(engine, table, row, types=types)
    engine.execute(table.insert(row))


def update_row(engine, table, row, unique, ensure=False, types={}):
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

def upsert(engine, table, row, unique, ensure=False, types={}):
    if not update_row(engine, table, row, unique, ensure=ensure, types=types):
        add_row(engine, table, row, ensure=ensure, types=types)

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



