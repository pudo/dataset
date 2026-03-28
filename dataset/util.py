from collections import OrderedDict
from hashlib import sha1
from urllib.parse import urlencode, urlparse

from sqlalchemy.exc import ResourceClosedError

QUERY_STEP = 1000
row_type = OrderedDict

try:
    # SQLAlchemy > 1.4.0, new row model.
    from sqlalchemy.engine import Row  # noqa

    def convert_row(row_type, row):
        if row is None:
            return None
        return row_type(row._mapping.items())


except ImportError:
    # SQLAlchemy < 1.4.0, no _mapping.

    def convert_row(row_type, row):
        if row is None:
            return None
        return row_type(row.items())


class DatasetError(Exception):
    pass


def iter_result_proxy(rp, step=None):
    """Iterate over the ResultProxy."""
    while True:
        chunk = rp.fetchall() if step is None else rp.fetchmany(size=step)
        if not chunk:
            break
        yield from chunk


def make_sqlite_url(
    path,
    cache=None,
    timeout=None,
    mode=None,
    check_same_thread=True,
    immutable=False,
    nolock=False,
):
    # NOTE: this PR
    # https://gerrit.sqlalchemy.org/c/sqlalchemy/sqlalchemy/+/1474/
    # added support for URIs in SQLite
    # The full list of supported URIs is a combination of:
    # https://docs.python.org/3/library/sqlite3.html#sqlite3.connect
    # and
    # https://www.sqlite.org/uri.html
    params = {}
    if cache:
        assert cache in ("shared", "private")
        params["cache"] = cache
    if timeout:
        # Note: if timeout is None, it uses the default timeout
        params["timeout"] = timeout
    if mode:
        assert mode in ("ro", "rw", "rwc")
        params["mode"] = mode
    if nolock:
        params["nolock"] = 1
    if immutable:
        params["immutable"] = 1
    if not check_same_thread:
        params["check_same_thread"] = "false"
    if not params:
        return "sqlite:///" + path
    params["uri"] = "true"
    return "sqlite:///file:" + path + "?" + urlencode(params)


class ResultIter:
    """SQLAlchemy ResultProxies are not iterable to get a
    list of dictionaries. This is to wrap them."""

    def __init__(self, result_proxy, row_type=row_type, step=None, connection=None):
        self.row_type = row_type
        self.result_proxy = result_proxy
        self._conn = connection
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
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def normalize_column_name(name):
    """Check if a string is a reasonable thing to use as a column name."""
    if not isinstance(name, str):
        raise ValueError(f"{name!r} is not a valid column name.")

    # limit to 63 characters
    name = name.strip()[:63]
    # column names can be 63 *bytes* max in postgresql
    if isinstance(name, str):
        while len(name.encode("utf-8")) >= 64:
            name = name[: len(name) - 1]

    if not len(name) or "." in name or "-" in name:
        raise ValueError(f"{name!r} is not a valid column name.")
    return name


def normalize_column_key(name):
    """Return a comparable column name."""
    if name is None or not isinstance(name, str):
        return None
    return name.upper().strip().replace(" ", "")


def normalize_table_name(name):
    """Check if the table name is obviously invalid."""
    if not isinstance(name, str):
        raise ValueError(f"Invalid table name: {name!r}")
    name = name.strip()[:63]
    if not len(name):
        raise ValueError(f"Invalid table name: {name!r}")
    return name


def safe_url(url):
    """Remove password from printed connection URLs."""
    parsed = urlparse(url)
    if parsed.password is not None:
        pwd = f":{parsed.password}@"
        url = url.replace(pwd, ":*****@")
    return url


def index_name(table, columns):
    """Generate an artificial index name."""
    sig = "||".join(columns)
    key = sha1(sig.encode("utf-8")).hexdigest()[:16]
    return f"ix_{table}_{key}"


def pad_chunk_columns(chunk, columns):
    """Given a set of items to be inserted, make sure they all have the
    same columns by padding columns with None if they are missing."""
    for record in chunk:
        for column in columns:
            record.setdefault(column, None)
    return chunk
