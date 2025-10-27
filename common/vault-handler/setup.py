"""Setup for vault-handler package."""

from setuptools import setup, find_packages

setup(
    name='vault-handler',
    version='0.1.0',
    description='HashiCorp Vault handler for reading secrets',
    packages=find_packages(),
    install_requires=[
        'hvac>=2.1.0',
    ],
    python_requires='>=3.10',
)
