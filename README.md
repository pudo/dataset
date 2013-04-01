SQLAlchemy Loading Tools
========================

A collection of wrappers and functions to make SQLAlchemy core easier 
to use in ETL applications. SQLAlchemy is used only for database
abstraction and not as an ORM, allowing users to write extraction
scripts that can work with multiple database backends. Functions
include:

* **Automatic schema**. If a column is written that does not
  exist on the table, it will be created automatically.
* **Upserts**. Records are either created or updated, depdending on
  whether an existing version can be found.
* **Query helpers** for simple queries such as all rows in a table or
  all distinct values across a set of columns.

Examples
--------

A typical use of ``sqlaload`` would look like this:

	from sqlaload import connect, get_table, distinct, update
    
	engine = connect('sqlite:///customers.db')
	table = get_table(engine, 'customers')
	for entry in distinct(engine, table, 'post_code', 'city')
    	lon, lat = geocode(entry['post_code'], entry['city'])
	    update(entry, {'lon': lon, 'lat': lat})

In this example, we selected all distinct post codes and city names from an imaginary customers database, send them through our geocoding routine and finally updated all matching rows with the returned geo information.

Another example, updating data in a datastore, might look like this:

	from sqlaload import connect, get_table, upsert
    
	engine = connect('sqlite:///things.db')
	table = get_table(engine, 'data')
    
	for item in magic_data_source_that_produces_entries():
    	assert 'key1' in item
	    assert 'key2' in item
	    # this will either insert or update, depending on 
	    # whether an entry with the matching values for 
	    # 'key1' and 'key2' already exists:
    	upsert(engine, table, item, ['key1', 'key2'])


Here's the same example, but using the object-oriented API:

    import sqlaload

    db = sqlaload.create('sqlite:///things.db')
    table = db.get_table('data')

    for item in magic_data_source_that_produces_entries():
        assert 'key1' in item
        assert 'key2' in item
        table.upsert(item, ['key1', 'key2'])


Functions
---------

The library currently exposes the following functions:

**Schema management**

* ``connect(url)``, connect to a database and return an ``engine``. See the [SQLAlchemy documentation](http://docs.sqlalchemy.org/en/rel_0_8/core/engines.html#database-urls) for information about URL schemes and formats.
* ``get_table(engine, table_name)`` will load a table configuration from the database, either reflecting the existing schema or creating a new table (with an ``id`` column).
* ``create_table(engine, table_name)`` and ``load_table(engine, table_name)`` are more explicit than ``get_table`` but allow the same functions.
* ``drop_table(engine, table_name)`` will remove an existing table, deleting all of its contents.
* ``create_column(engine, table, column_name, type)`` adds a new column to a table, ``type`` must be a SQLAlchemy type class.
* ``create_index(engine, table, columns)`` creates an index on the given table, based on a list of strings to specify the included ``columns``.

**Queries**

* ``find(engine, table, _limit=N, _offset=N, order_by='id', **kw)`` will retrieve database records. The query will return an iterator that only loads 5000 records at any one time, even if ``_limit`` and ``_offset`` are specified - meaning that ``find`` can be run on tables of arbitrary size. ``order_by`` is a string column name, always returned in ascending order. Finally ``**kw`` can be used to filter columns for equality, e.g. ``find(…, category=5)``. 
* ``find_one(engine, table, **kw)``, like ``find`` but will only return the first matching row or ``None`` if no matches were found. 
* ``distinct(engine, table, *columns, **kw)`` will return the combined distinct values for ``columns``. ``**kw`` allows filtering the same way it does in ``find``.
* ``all``, alias for ``find`` without filter options.

**Adding and updating data**

* ``add_row(engine, table, row, ensure=True, types={})`` add the values in the dictionary ``row`` to the given ``table``. ``ensure`` will check the schema and create the columns if necessary, their types can be specified using the ``types`` dictionary. If no ``types`` are given, the type will be guessed from the first submitted value of the column, defaulting to a text column. 
* ``update_row(engine, table, row, unique, ensure=True, types={})`` will update a row or set of rows based on the data in the ``row`` dictionary and the column names specified in ``unique``. The remaining arguments are handled like those in ``add_row``. 
* ``upsert(engine, table, row, unique, ensure=True, types={})`` will combine the semantics of ``update_row`` and ``add_row`` by first attempting to update existing data and otherwise (only if no record matching on the ``unique`` keys can be found) creating a new record.
* ``delete(engine, table, **kw)`` will remove records from a table. ``**kw`` is the same as in ``find`` and can be used to limit the set of records to be removed.



Feedback
--------

Please feel free create issues on the GitHub tracker at [okfn/sqlaload](https://github.com/okfn/sqlaload/issues). For other discussions, join the [okfn-labs](http://lists.okfn.org/mailman/listinfo/okfn-labs) mailing list. 


