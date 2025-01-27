#!/usr/bin/env python
from setuptools import setup

setup(
    name="django-mysql-s3-backup",
    author="Dan Paulson",
    author_email="danpaulson@gmail.com",
    description="Simple backup/restore to s3 for mysql",
    version='1.0.9',
    install_requires=[
        'boto3',
        'prompt_toolkit',
    ]
)
