import sys
from setuptools import setup, find_packages


py26_dependency = []
if sys.version_info <= (2, 6):
    py26_dependency = ["argparse >= 1.2.1"]

setup(
    name='dataset',
    version='0.3.15',
    description="Toolkit for Python-based data processing.",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3'
    ],
    keywords='sql sqlalchemy etl loading utility',
    author='Friedrich Lindenberg, Gregor Aisch',
    author_email='info@okfn.org',
    url='http://github.com/pudo/dataset',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=[],
    include_package_data=False,
    zip_safe=False,
    install_requires=[
        'sqlalchemy >= 0.8.1',
        'alembic >= 0.6.1',
        'python-slugify >= 0.0.6',
        "PyYAML >= 3.10"
    ] + py26_dependency,
    tests_require=[],
    entry_points={
        'console_scripts': [
            'datafreeze = dataset.freeze.app:main',
        ]
    }
)
