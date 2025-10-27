"""Setup for s3-handler package."""

from setuptools import setup, find_packages

setup(
    name='s3-handler',
    version='0.1.0',
    description='S3/MinIO handler for object storage operations',
    packages=find_packages(),
    install_requires=[
        'boto3>=1.34.0',
    ],
    python_requires='>=3.10',
)
