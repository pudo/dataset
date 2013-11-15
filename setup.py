from setuptools import setup, find_packages

setup(
    name='dataset',
    version='0.3.14',
    description="Toolkit for Python-based data processing.",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
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
        'sqlalchemy-migrate >= 0.7',
        "argparse >= 1.2.1",
        'python-slugify >= 0.0.6',
        "PyYAML >= 3.10"
    ],
    tests_require=[],
    entry_points={
        'console_scripts': [
            'datafreeze = dataset.freeze.app:main',
        ]
    }
)
