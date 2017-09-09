try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

try:
    from collections import OrderedDict
except ImportError:  # pragma: no cover
    from ordereddict import OrderedDict

from six import string_types
from collections import Sequence
from hashlib import sha1

QUERY_STEP = 1000
row_type = OrderedDict


class DatasetException(Exception):
    pass


def convert_row(row_type, row):
    if row is None:
        return None
    return row_type(row.items())


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
        self.result_proxy = result_proxy
        self.keys = list(result_proxy.keys())
        self._iter = iter_result_proxy(result_proxy, step=step)

    def __next__(self):
        return convert_row(self.row_type, next(self._iter))

    next = __next__

    def __iter__(self):
        return self

    def close(self):
        self.result_proxy.close()


def normalize_column_name(name):
    """Check if a string is a reasonable thing to use as a column name."""
    if not isinstance(name, string_types):
        raise ValueError('%r is not a valid column name.' % name)
    name = name.strip()
    if not len(name) or '.' in name or '-' in name:
        raise ValueError('%r is not a valid column name.' % name)
    return name


def normalize_table_name(name):
    """Check if the table name is obviously invalid."""
    if not isinstance(name, string_types):
        raise ValueError("Invalid table name: %r" % name)
    name = name.strip()
    if not len(name):
        raise ValueError("Invalid table name: %r" % name)
    return name


def safe_url(url):
    """Remove password from printed connection URLs."""
    parsed = urlparse(url)
    if parsed.password is not None:
        pwd = ':%s@' % parsed.password
        url = url.replace(pwd, ':*****@')
    return url


def index_name(table, columns):
    """Generate an artificial index name."""
    sig = '||'.join(columns)
    key = sha1(sig.encode('utf-8')).hexdigest()[:16]
    return 'ix_%s_%s' % (table, key)


def ensure_tuple(obj):
    """Try and make the given argument into a tuple."""
    if obj is None:
        return tuple()
    if isinstance(obj, Sequence) and not isinstance(obj, string_types):
        return tuple(obj)
    return obj,
