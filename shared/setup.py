"""
Setup configuration for shared handlers package.
"""

from setuptools import setup, find_packages

setup(
    name='pipeline-handlers',
    version='0.1.0',
    description='Shared handlers for data pipeline containers',
    author='Data Engineering Team',
    packages=find_packages(),
    install_requires=[
        'hvac>=2.1.0',
        'boto3>=1.34.0',
        'psycopg2-binary>=2.9.0',
        'trino>=0.328.0',
    ],
    python_requires='>=3.10',
)
