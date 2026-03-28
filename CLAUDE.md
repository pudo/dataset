# dataset: databases for lazy people

## Project Overview

**dataset** is a lightweight Python library that makes reading and writing data in databases as simple as working with JSON files. It provides a simple abstraction layer on top of SQLAlchemy, removing most direct SQL statements without requiring a full ORM model.

**Key Philosophy:** Simplicity over complexity. The library is designed for small to medium-scale datasets where you want database benefits without the overhead of complex ORM patterns.

## Repository Structure

```
dataset/
├── dataset/              # Main package
│   ├── __init__.py      # Main entry point with connect()
│   ├── database.py      # Database class - connection, transactions, queries
│   ├── table.py         # Table class - CRUD operations, schema management
│   ├── types.py         # SQLAlchemy type mapping and helpers
│   ├── util.py          # Utility functions (ResultIter, normalization, etc.)
│   └── chunked.py       # Chunked operations for large datasets
├── test/                # Test suite (pytest)
│   ├── conftest.py      # Test fixtures
│   ├── test_database.py
│   ├── test_table.py
│   └── test_row_type.py
├── docs/                # Sphinx documentation (RST format)
└── pyproject.toml       # Package configuration (PEP 621)
```

## Core Concepts

### 1. Database Connection
- Single entry point: `dataset.connect(url)` returns a `Database` instance
- Supports SQLite (default), PostgreSQL, and MySQL
- Connection pooling managed by SQLAlchemy
- Environment variable fallback: `DATABASE_URL`

### 2. Automatic Schema Management
- **Default behavior:** Tables and columns are created automatically on insert
- Can be disabled via `ensure_schema=False` parameter
- Column types are guessed from values (or explicitly specified)
- Thread-safe with locking for schema operations

### 3. CRUD Operations
- **insert()**: Add single row, returns primary key
- **insert_many()**: Bulk insert with chunking (default: 1000 rows)
- **update()**: Update rows matching keys
- **upsert()**: Insert or update based on key columns
- **delete()**: Delete rows matching filters
- **find()**: Query with filters, ordering, limit/offset
- **find_one()**: Return single row or None

### 4. Transaction Support
- Context manager: `with dataset.connect() as tx:`
- Manual control: `db.begin()`, `db.commit()`, `db.rollback()`
- Nested transactions supported
- SQLAlchemy 2.x "autobegin" semantics with explicit commit tracking

## Important Implementation Details

### Threading & Concurrency
- Each thread gets its own database connection (thread-local storage)
- Schema changes in transactions with multiple threads trigger warnings
- Locking strategy:
  - `self.lock` (RLock) protects metadata operations
  - Keep lock scope small for performance
  - Schema operations are fully synchronized

### Transaction Handling (SQLAlchemy 2.x Migration)
- The library supports both SQLAlchemy 1.4+ and 2.x
- Key change: SQLAlchemy 2.x uses "autobegin" (transactions start on first use)
- `db._auto_commit()` commits after writes when not in explicit transaction
- Transaction nesting tracked via `self.local.tx` stack

### Column & Table Name Normalization
- Case-insensitive column matching via `normalize_column_key()`
- Actual database names preserved via `_column_keys` mapping
- Tables and columns validated for safety

### Type System
- `db.types` provides shortcuts to SQLAlchemy types
- Type guessing from Python values: `types.guess(value)`
- Custom types via `types` parameter in insert/update
- MySQL-specific: text field indexing uses 10-char prefix

## Testing Strategy

### Test Infrastructure
- **Framework:** pytest
- **Fixtures:**
  - `db`: Function-scoped database connection with cleanup
  - `table`: Pre-populated weather table with test data
- **CI/CD:** GitHub Actions testing against SQLite, PostgreSQL, MySQL
- **Database Cleanup:** Explicit rollback + drop tables in teardown

### Running Tests
```bash
make test                    # Run all tests
pytest test/test_table.py    # Run specific test file
DATABASE_URL="postgresql://..." pytest  # Test against specific DB
```

### Test Database Configuration
- Default: SQLite in-memory (`:memory:`)
- Override via `DATABASE_URL` environment variable
- CI uses Docker containers for PostgreSQL and MySQL

## Code Style & Quality

### Tools
- **Build System:** Hatchling (modern PEP 621 compliant)
- **Linting & Formatting:** Ruff with default recommended rules
- **Type Checking:** mypy (cache in `.mypy_cache/`)

### Ruff Configuration
The project uses ruff's default rule sets including:

```bash
make lint          # Check for linting issues
make format-check  # Check formatting without applying
make format        # Apply formatting
```

The codebase has been fully formatted and all linting rules pass.

### Best Practices
1. **Keep it simple:** The library's strength is simplicity - don't over-engineer
2. **Thread safety:** Always use locks for schema operations
3. **Auto-commit:** Remember to call `db._auto_commit()` after writes outside transactions
4. **Column normalization:** Use `_get_column_name()` for case-insensitive matching
5. **Error handling:** Use `DatasetError` for dataset-specific errors (renamed from DatasetException)
6. **Exception naming:** Use `Error` suffix for exception classes (e.g., `InvalidCallbackError`)
7. **Linting:** Run `make lint` before committing

## Common Development Tasks

### Adding a New Table Method
1. Add method to `Table` class in [table.py](dataset/table.py)
2. Use `self._sync_columns()` if it modifies schema
3. Call `self.db._auto_commit()` after database writes
4. Add docstring with example code
5. Update [docs/api.rst](docs/api.rst) if needed
6. Write tests in [test/test_table.py](test/test_table.py)

### Adding a New Query Operator
1. Add operator to `_generate_clause()` in [table.py](dataset/table.py:398-430)
2. Document in [docs/queries.rst](docs/queries.rst)
3. Add test cases for the operator

### Modifying Transaction Behavior
- Be careful: both nested and non-nested transactions must work
- Test with multiple threads
- Transaction state tracked in `self.local.tx` (thread-local)
- See `begin()`, `commit()`, `rollback()` in [database.py](dataset/database.py:134-175)

## Known Limitations

**Explicitly out of scope:**
- Foreign key relationships and ORM-style relations
- Python-wrapped JOIN queries
- Database creation or DBMS management
- Python 2.x support (dropped in 1.2.0)
- Async operations
- Database-native UPSERT (library implements via SELECT + INSERT/UPDATE)

**Technical constraints:**
- SQLite doesn't support dropping columns
- MySQL requires prefix length for text field indexes
- Very large text fields may not be fully indexed

## Migration Notes

### Modern Build System (v1.7+)
- Migrated from setuptools to Hatchling with pyproject.toml (PEP 621)
- Use `python -m build` instead of `setup.py sdist bdist_wheel`
- Version automatically read from `dataset/__init__.py`
- All configuration now in `pyproject.toml`

### SQLAlchemy 2.x Support (v1.6+)
- Supports SQLAlchemy 1.4.0 through 2.x
- Key changes: autobegin semantics, connection handling
- `_auto_commit()` method handles post-write commits
- Transaction tracking via `in_transaction` property

### Breaking Changes (v1.0)
- Data export features extracted to separate `datafreeze` package
- Text-based `primary_type` dropped - use `db.types` instead
- Advanced `__getitem__` syntax for primary keys removed

## Dependencies

**Core:**
- `sqlalchemy >= 1.4.0, < 3.0.0` - Database abstraction
- `alembic >= 0.6.2` - Schema migrations
- `banal >= 1.0.1` - Utility functions

**Development:**
- `pytest` - Testing framework
- `ruff` - Linting and formatting
- `build` - Modern Python package builder
- `twine` - Package upload to PyPI
- `psycopg2-binary` - PostgreSQL driver
- `PyMySQL` - MySQL driver
- `cryptography` - For secure connections

## Documentation

- **Official docs:** https://dataset.readthedocs.io/
- **Source:** [docs/](docs/) folder (Sphinx/RST format)
- Build docs: `cd docs && make html`

## Release Process

1. Update version in [dataset/__init__.py](dataset/__init__.py:14)
2. Run `bumpversion patch` (or `minor`/`major`) to update version everywhere
3. Update [CHANGELOG.md](CHANGELOG.md) with release notes
4. Run full test suite: `make test`
5. Run linting: `make lint`
6. Build distributions: `make dists` (creates wheel and sdist)
7. Tag release: `git tag -a v1.x.x -m "Release 1.x.x"`
8. Push commits and tag: `git push && git push --tags`
9. GitHub Actions will automatically publish to PyPI on tag push

## Special Considerations for AI Assistants

### When Modifying Code
- **Test thoroughly:** Changes affect SQLite, PostgreSQL, AND MySQL
- **Thread safety:** Schema changes require locking
- **Backward compatibility:** This is a stable library used by many projects
- **Document examples:** Include usage examples in docstrings

### When Debugging
- Check which database backend is being used (`db.is_sqlite`, `db.is_postgres`, `db.is_mysql`)
- Look at thread-local state (`self.local.tx`) for transaction issues
- Column name mismatches often due to case sensitivity - check `_column_keys`
- SQLAlchemy version differences may affect behavior

### Common Pitfalls
1. Forgetting to call `_auto_commit()` after writes
2. Not using locks for schema operations
3. Assuming case-sensitive column names
4. Not handling the difference between SQLite/PostgreSQL/MySQL
5. Breaking nested transaction semantics

## Use Cases

**dataset** is ideal for:
- Quick data loading scripts
- Simple ETL operations
- Testing and prototyping
- Small utility databases
- Data exploration and analysis
- Situations where ORM overhead isn't needed

When to use full SQLAlchemy ORM instead:
- Complex relational models with foreign keys
- Need for relationship management
- Advanced query patterns with JOINs
- Large-scale production applications
