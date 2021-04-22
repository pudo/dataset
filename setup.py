from setuptools import setup, find_packages

with open("README.md") as f:
    long_description = f.read()

setup(
    name="dataset",
    version="1.5.0",
    description="Toolkit for Python-based database access.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="sql sqlalchemy etl loading utility",
    author="Friedrich Lindenberg, Gregor Aisch, Stefan Wehrmeyer",
    author_email="friedrich.lindenberg@gmail.com",
    url="http://github.com/pudo/dataset",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "test"]),
    namespace_packages=[],
    include_package_data=False,
    zip_safe=False,
    install_requires=["sqlalchemy >= 1.3.2", "alembic >= 0.6.2", "banal >= 1.0.1"],
    extras_require={
        "dev": [
            "pip",
            "nose",
            "wheel",
            "flake8",
            "coverage",
            "psycopg2-binary",
            "PyMySQL",
            "cryptography",
        ]
    },
    tests_require=["nose"],
    test_suite="test",
    entry_points={},
)
