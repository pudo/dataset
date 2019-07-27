from setuptools import setup, find_packages


setup(
    name='dataset',
    version='1.1.2',
    description="Toolkit for Python-based database access.",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='sql sqlalchemy etl loading utility',
    author='Friedrich Lindenberg, Gregor Aisch, Stefan Wehrmeyer',
    author_email='friedrich@pudo.org',
    url='http://github.com/pudo/dataset',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    include_package_data=False,
    zip_safe=False,
    install_requires=[
        'sqlalchemy >= 1.1.2',
        'alembic >= 0.6.2',
        "six >= 1.11.0",
        'sqlalchemy_utils'
    ],
    tests_require=[
        'nose'
    ],
    test_suite='test',
    entry_points={}
)
