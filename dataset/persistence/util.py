from datetime import datetime, date
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict

from sqlalchemy import Integer, UnicodeText, Float, Date, DateTime, Boolean
from six import string_types

row_type = OrderedDict


def guess_type(sample):
    if isinstance(sample, bool):
        return Boolean
    elif isinstance(sample, int):
        return Integer
    elif isinstance(sample, float):
        return Float
    elif isinstance(sample, datetime):
        return DateTime
    elif isinstance(sample, date):
        return Date
    return UnicodeText


def convert_row(row_type, row):
    if row is None:
        return None
    return row_type(row.items())


def normalize_column_name(name):
    if not isinstance(name, string_types):
        raise ValueError('%r is not a valid column name.' % name)
    name = name.lower().strip()
    if not len(name) or '.' in name or '-' in name:
        raise ValueError('%r is not a valid column name.' % name)
    return name


def iter_result_proxy(rp, step=None):
    """Iterate over the ResultProxy."""
    while True:
        if step is None:
            chunk = rp.fetchall()
        else:
            chunk = rp.fetchmany(step)
        if not chunk:
            break
        for row in chunk:
            yield row


class ResultIter(object):
    """ SQLAlchemy ResultProxies are not iterable to get a
    list of dictionaries. This is to wrap them. """

    def __init__(self, result_proxy, row_type=row_type, step=None):
        self.row_type = row_type
        self.keys = list(result_proxy.keys())
        self._iter = iter_result_proxy(result_proxy, step=step)

    def __next__(self):
        return convert_row(self.row_type, next(self._iter))

    next = __next__

    def __iter__(self):
        return self


def safe_url(url):
    """Remove password from printed connection URLs."""
    parsed = urlparse(url)
    if parsed.password is not None:
        pwd = ':%s@' % parsed.password
        url = url.replace(pwd, ':*****@')
    return url
