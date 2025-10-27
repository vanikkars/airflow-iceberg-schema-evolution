"""Setup for postgres-handler package."""

from setuptools import setup, find_packages

setup(
    name='postgres-handler',
    version='0.1.0',
    description='PostgreSQL handler for database operations',
    packages=find_packages(),
    install_requires=[
        'psycopg2-binary>=2.9.0',
    ],
    python_requires='>=3.10',
)
