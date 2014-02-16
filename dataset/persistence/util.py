from datetime import datetime, timedelta
from inspect import isgenerator

from sqlalchemy import Integer, UnicodeText, Float, DateTime, Boolean, types, Table, event


def guess_type(sample):
    if isinstance(sample, bool):
        return Boolean
    elif isinstance(sample, int):
        return Integer
    elif isinstance(sample, float):
        return Float
    elif isinstance(sample, datetime):
        return DateTime
    return UnicodeText


class ResultIter(object):
    """ SQLAlchemy ResultProxies are not iterable to get a
    list of dictionaries. This is to wrap them. """

    def __init__(self, result_proxies):
        if not isgenerator(result_proxies):
            result_proxies = iter((result_proxies, ))
        self.result_proxies = result_proxies
        self.count = 0
        self.rp = None

    def _next_rp(self):
        try:
            self.rp = next(self.result_proxies)
            self.count += self.rp.rowcount
            self.keys = list(self.rp.keys())
            return True
        except StopIteration:
            return False

    def __next__(self):
        if self.rp is None:
            if not self._next_rp():
                raise StopIteration
        row = self.rp.fetchone()
        if row is None:
            if self._next_rp():
                return next(self)
            else:
                # stop here
                raise StopIteration
        return row

    next = __next__

    def __iter__(self):
        return self


def sqlite_datetime_fix():
    class SQLiteDateTimeType(types.TypeDecorator):
        impl = types.Integer
        epoch = datetime(1970, 1, 1, 0, 0, 0)

        def process_bind_param(self, value, dialect):
            if isinstance(value, datetime):
                return value
            return (value / 1000 - self.epoch).total_seconds()

        def process_result_value(self, value, dialect):
            return self.epoch + timedelta(seconds=value / 1000)

    def is_sqlite(inspector):
        return inspector.engine.dialect.name == "sqlite"

    def is_datetime(column_info):
        return isinstance(column_info['type'], types.DateTime)

    @event.listens_for(Table, "column_reflect")
    def setup_epoch(inspector, table, column_info):
        if is_sqlite(inspector) and is_datetime(column_info):
            column_info['type'] = SQLiteDateTimeType()
