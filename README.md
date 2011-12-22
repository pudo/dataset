SQLAlchemy Loading Tools
========================

A collection of wrappers and functions to make SQLAlchemy core easier 
to use in an ETL application. The package is used only for database
abstraction and not as an ORM, allowing users to write extraction
scripts that can work with multiple database backends. Functions
include:

* **Self-expanding schema**. If a column is written that does not
  exist on the table, it will be created automatically.
* **Upserts**. Records are either created or updated, depdending on
  whether an existing version can be found.
* **Query helpers** for simple queries such as all rows in a table or
  all distinct values of a set of columns.


Example
-------

A typical use case for ``sqlaload`` may include code like this::

    from sqlaload import connect, get_table, distinct, update
    
    engine = connect('sqlite:///customers.db')
    table = get_table('customers')
    for entry in distinct(engine, table, 'post_code', 'city')
        lon, lat = geocode(entry['post_code'], entry['city'])
        update(entry, {'lon': lon, 'lat': lat})

In this example, we selected all distinct post codes and city names
from an imaginary customers database, sent them through our 
geocoding routine and finally updated all matching rows with our 
geo information.

Another example, updating data in a datastore, might look like 
this::

    from sqlaload import connect, get_table, upsert
    
    engine = connect('sqlite:///things.db')
    table = get_table('data')
    
    for item in magic_data_source_that_produces_entries():
        assert 'key1' in item
        assert 'key2' in item
        # this will either insert or update, depending on 
        # whether an entry with the matching values for 
        # 'key1' and 'key2' already exists:
        upsert(engine, table, item, ['key1', 'key2'])


Feedback
--------

Please feel free create issues on the GitHub bug tracker at:

* https://github.com/pudo/sqlaload/issues


