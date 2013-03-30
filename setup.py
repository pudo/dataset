from setuptools import setup, find_packages

long_desc = open('README.md', 'r').read().split('****', 1)[0]

setup(
    name='sqlaload',
    version='0.2.1',
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
