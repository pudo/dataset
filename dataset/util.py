from hashlib import sha1
from urllib.parse import urlparse
from collections import OrderedDict
from sqlalchemy.exc import ResourceClosedError

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
            chunk = rp.fetchmany(size=step)
        if not chunk:
            break
        for row in chunk:
            yield row


class ResultIter(object):
    """SQLAlchemy ResultProxies are not iterable to get a
    list of dictionaries. This is to wrap them."""

    def __init__(self, result_proxy, row_type=row_type, step=None):
        self.row_type = row_type
        self.result_proxy = result_proxy
        try:
            self.keys = list(result_proxy.keys())
            self._iter = iter_result_proxy(result_proxy, step=step)
        except ResourceClosedError:
            self.keys = []
            self._iter = iter([])

    def __next__(self):
        try:
            return convert_row(self.row_type, next(self._iter))
        except StopIteration:
            self.close()
            raise

    next = __next__

    def __iter__(self):
        return self

    def close(self):
        self.result_proxy.close()


def normalize_column_name(name):
    """Check if a string is a reasonable thing to use as a column name."""
    if not isinstance(name, str):
        raise ValueError("%r is not a valid column name." % name)

    # limit to 63 characters
    name = name.strip()[:63]
    # column names can be 63 *bytes* max in postgresql
    if isinstance(name, str):
        while len(name.encode("utf-8")) >= 64:
            name = name[: len(name) - 1]

    if not len(name) or "." in name or "-" in name:
        raise ValueError("%r is not a valid column name." % name)
    return name


def normalize_column_key(name):
    """Return a comparable column name."""
    if name is None or not isinstance(name, str):
        return None
    return name.upper().strip().replace(" ", "")


def normalize_table_name(name):
    """Check if the table name is obviously invalid."""
    if not isinstance(name, str):
        raise ValueError("Invalid table name: %r" % name)
    name = name.strip()[:63]
    if not len(name):
        raise ValueError("Invalid table name: %r" % name)
    return name


def safe_url(url):
    """Remove password from printed connection URLs."""
    parsed = urlparse(url)
    if parsed.password is not None:
        pwd = ":%s@" % parsed.password
        url = url.replace(pwd, ":*****@")
    return url


def index_name(table, columns):
    """Generate an artificial index name."""
    sig = "||".join(columns)
    key = sha1(sig.encode("utf-8")).hexdigest()[:16]
    return "ix_%s_%s" % (table, key)


def pad_chunk_columns(chunk, columns):
    """Given a set of items to be inserted, make sure they all have the
    same columns by padding columns with None if they are missing."""
    for record in chunk:
        for column in columns:
            record.setdefault(column, None)
    return chunk
