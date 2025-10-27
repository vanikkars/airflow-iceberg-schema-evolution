"""Setup for trino-handler package."""

from setuptools import setup, find_packages

setup(
    name='trino-handler',
    version='0.1.0',
    description='Trino handler for query execution',
    packages=find_packages(),
    install_requires=[
        'trino>=0.328.0',
    ],
    python_requires='>=3.10',
)
