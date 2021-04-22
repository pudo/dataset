import os
import warnings
from dataset.database import Database
from dataset.table import Table
from dataset.util import row_type

# shut up useless SA warning:
warnings.filterwarnings("ignore", "Unicode type received non-unicode bind param value.")
warnings.filterwarnings(
    "ignore", "Skipping unsupported ALTER for creation of implicit constraint"
)

__all__ = ["Database", "Table", "connect"]
__version__ = "1.5.0"


def connect(
    url=None,
    schema=None,
    engine_kwargs=None,
    ensure_schema=True,
    row_type=row_type,
    sqlite_wal_mode=True,
):
    """Opens a new connection to a database.

    *url* can be any valid `SQLAlchemy engine URL`_.  If *url* is not defined
    it will try to use *DATABASE_URL* from environment variable.  Returns an
    instance of :py:class:`Database <dataset.Database>`. Additionally,
    *engine_kwargs* will be directly passed to SQLAlchemy, e.g. set
    *engine_kwargs={'pool_recycle': 3600}* will avoid `DB connection timeout`_.
    Set *row_type* to an alternate dict-like class to change the type of
    container rows are stored in.::

        db = dataset.connect('sqlite:///factbook.db')

    One of the main features of `dataset` is to automatically create tables and
    columns as data is inserted. This behaviour can optionally be disabled via
    the `ensure_schema` argument. It can also be overridden in a lot of the
    data manipulation methods using the `ensure` flag.

    .. _SQLAlchemy Engine URL: http://docs.sqlalchemy.org/en/latest/core/engines.html#sqlalchemy.create_engine
    .. _DB connection timeout: http://docs.sqlalchemy.org/en/latest/core/pooling.html#setting-pool-recycle
    """
    if url is None:
        url = os.environ.get("DATABASE_URL", "sqlite://")

    return Database(
        url,
        schema=schema,
        engine_kwargs=engine_kwargs,
        ensure_schema=ensure_schema,
        row_type=row_type,
        sqlite_wal_mode=sqlite_wal_mode,
    )
