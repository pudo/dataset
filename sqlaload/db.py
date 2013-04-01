

from sqlaload.schema import connect, get_table, create_table, load_table, drop_table, create_column, create_index
from sqlalchemy.engine import Engine
from sqlaload.write import add_row, update_row
from sqlaload.write import upsert, update, delete
from sqlaload.query import distinct, all, find_one, find, query


class DB(object):
    def __init__(self, engine):
        if isinstance(engine, (str, unicode)):
            self.engine = connect(engine)
        elif isinstance(engine, Engine):
            self.engine = engine
        else:
            raise Exception('unknown engine format')

    def create_table(self, table_name):
        return Table(self.engine, create_table(self.engine, table_name))

    def load_table(self, table_name):
        return Table(self.engine, load_table(self.engine, table_name))

    def get_table(self, table_name):
        return Table(self.engine, get_table(self.engine, table_name))

    def drop_table(self, table_name):
        drop_table(self.engine, table_name)

    def query(self, sql):
        return query(self.engine, sql)


class Table(object):
    def __init__(self, engine, table):
        self.engine = engine
        self.table = table

    # sqlaload.write

    def add_row(self, *args, **kwargs):
        return add_row(self.engine, self.table, *args, **kwargs)

    def update_row(self, *args, **kwargs):
        return update_row(self.engine, self.table, *args, **kwargs)

    def upsert(self, *args, **kwargs):
        return upsert(self.engine, self.table, *args, **kwargs)

    def update(self, *args, **kwargs):
        return update(self.engine, self.table, *args, **kwargs)

    def delete(self, *args, **kwargs):
        return delete(self.engine, self.table, *args, **kwargs)

    # sqlaload.query

    def find_one(self, *args, **kwargs):
        return find_one(self.engine, self.table, *args, **kwargs)

    def find(self, *args, **kwargs):
        return find(self.engine, self.table, *args, **kwargs)

    def all(self):
        return all(self.engine, self.table)

    def distinct(self, *args, **kwargs):
        return distinct(self.engine, self.table, *args, **kwargs)

    # sqlaload.schema

    def create_column(self, name, type):
        return create_column(self.engine, self.table, name, type)

    def create_index(self, columns, name=None):
        return create_index(self.engine, self.table, columns, name=name)


def create(engine):
    """ returns a DB object """
    return DB(engine)
