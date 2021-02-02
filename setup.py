#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-amazon-sp",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_amazon_sp"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.9.1",
        "python-amazon-sp-api==0.1.9"
    ],
    entry_points="""
    [console_scripts]
    tap-amazon-sp=tap_amazon_sp:main
    """,
    packages=["tap_amazon_sp", "tap_amazon_sp.streams"],
    package_data={
        "schemas": ["tap_amazon_sp/schemas/*.json"]
    },
    include_package_data=True,
)
