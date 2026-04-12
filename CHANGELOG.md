# dataset ChangeLog

*The changelog has only been started with version 0.3.12, previous
changes must be reconstructed from revision history.*

* **2.0.0**: Major modernization and type annotations
  - **Type annotations**: Full `mypy --strict` compliance across all modules
  - **PEP 561**: Added `py.typed` marker for downstream type checking
  - **New types**: Exported `OutRow`, `RowFactory`, `QueryError` for downstream use
  - **`RowFactory`**: The `row_type` parameter is now typed as `Callable[[Iterable[tuple[str, Any]]], OutRow]` instead of `type`
  - **`QueryError`**: New exception subclass of `DatasetError` for invalid filter operations
  - **`primary_type`**: Changed from `Types` to `ColumnType` (SQLAlchemy `TypeEngine`) — the actual accepted type
  - **`insert`/`insert_ignore`/`upsert`**: Return type changed from `int | bool` to `Any` (primary keys can be any type)
  - **Removed `banal` dependency**: Replaced `ensure_list` with typed `ensure_strings` utility
  - **`update_many`**: Fixed mutation of input rows — rows are now copied before modification
  - **Dev tooling**: Added `mypy` to dev dependencies, `make lint` now runs both ruff and mypy
  - **Build system**: Migrated from setuptools to modern pyproject.toml with Hatchling (PEP 621)
  - **Linting**: Replaced flake8 with ruff for faster, more comprehensive linting
  - **CI/CD**: Updated GitHub Actions to use modern action versions (checkout@v4, setup-python@v5)
  - **SQLAlchemy 2.x**: Full support for SQLAlchemy 2.0+ with backward compatibility to 1.4.0
  - **Transaction handling**: Fixed autobegin semantics and DDL lock contention for SQLAlchemy 2.x
  - **Testing**: Switched from nose to pytest, improved test fixtures and cleanup
  - **Database support**: Added lock timeout configurations for PostgreSQL and MySQL in CI
  - **Python support**: Now requires Python 3.10+, tested on 3.10-3.13
  - **Documentation**: Updated installation instructions, copyright year, and added comprehensive CLAUDE.md
  - **Metadata**: Changed development status from Alpha to Production/Stable
  - **License**: Renamed LICENSE.txt to LICENSE for standard convention
  - **Dependencies**: Updated SQLAlchemy constraint to allow versions up to 3.0.0
* 1.6.2: Fix distinct() to respect _limit and _offset parameters (#424).
* 1.6.1: Fix add_column method compatibility with Alembic 1.11+ (#423).
* 1.6.0: Pin SQLAlchemy below 2.0.0 for compatibility.
* 1.5.2: Consider primary key when checking for indexes (#382). Add missing arguments for query method (#391).
* 1.5.1: Improve row conversion compatibility across SQLAlchemy 1.3 and 1.4.
* 1.5.0: Add support for custom SQLite pragmas via `on_connect_statements` parameter. Switch from nose to pytest for testing.
* 1.2.0: Add support for views, multiple comparison operators.
  Remove support for Python 2.
* 1.1.0: Introduce `types` system to shortcut for SQLA types.
* 1.0.0: Massive re-factor and code cleanup.
* 0.6.0: Remove sqlite_datetime_fix for automatic int-casting of dates,
  make table['foo', 'bar'] an alias for table.distinct('foo', 'bar'),
  check validity of column and table names more thoroughly, rename
  reflectMetadata constructor argument to reflect_metadata, fix
  ResultIter to not leave queries open (so you can update in a loop).
* 0.5.7: dataset Databases can now have customized row types. This allows,
  for example, information to be retrieved in attribute-accessible dict
  subclasses, such as stuf.
* 0.5.4: Context manager for transactions, thanks to @victorkashirin.
* 0.5.1: Fix a regression where empty queries would raise an exception.
* 0.5: Improve overall code quality and testing, including Travis CI.
  An advanced __getitem__ syntax which allowed for the specification 
  of primary keys when getting a table was dropped. 
  DDL is no longer run against a transaction, but the base connection. 
* 0.4: Python 3 support and switch to alembic for migrations.
* 0.3.15: Fixes to update and insertion of data, thanks to @cli248
  and @abhinav-upadhyay.
* 0.3.14: dataset went viral somehow. Thanks to @gtsafas for
  refactorings, @alasdairnicol for fixing the Freezfile example in 
  the documentation. @diegoguimaraes fixed the behaviour of insert to
  return the newly-created primary key ID. table.find_one() now
  returns a dict, not an SQLAlchemy ResultProxy. Slugs are now generated
  using the Python-Slugify package, removing slug code from dataset. 
* 0.3.13: Fixed logging, added support for transformations on result
  rows to support slug generation in output (#28).
* 0.3.12: Makes table primary key's types and names configurable, fixing
  #19. Contributed by @dnatag.
