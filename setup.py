from setuptools import setup, find_packages

long_desc = """A collection of wrappers and functions to make SQLAlchemy core easier 
to use in ETL applications. SQLAlchemy is used only for database
abstraction and not as an ORM, allowing users to write extraction
scripts that can work with multiple database backends. Functions
include:

* **Automatic schema**. If a column is written that does not
  exist on the table, it will be created automatically.
* **Upserts**. Records are either created or updated, depdending on
  whether an existing version can be found.
* **Query helpers** for simple queries such as all rows in a table or
  all distinct values across a set of columns."""

setup(
    name='sqlaload',
    version='0.2.2',
    description="Utility functions for using SQLAlchemy in ETL.",
    long_description=long_desc,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        ],
    keywords='sql sqlalchemy etl loading utility',
    author='Open Knowledge Foundation',
    author_email='info@okfn.org',
    url='http://github.com/okfn/sqlaload',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=[],
    include_package_data=False,
    zip_safe=False,
    install_requires=[
        'sqlalchemy>=0.7',
        'sqlalchemy-migrate>=0.7'
    ],
    tests_require=[],
    entry_points=\
    """ """,
)
