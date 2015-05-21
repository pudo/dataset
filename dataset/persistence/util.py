from datetime import datetime, timedelta
from inspect import isgenerator

try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict

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


def convert_row(row):
    if row is None:
        return None
    return OrderedDict(row.items())


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
        return convert_row(row)

    next = __next__

    def __iter__(self):
        return self
