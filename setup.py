#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name="tap-airtable",
    version="0.0.4",
    description="Singer.io tap for extracting data from the Airtable API",
    author="AIME Mentorinng",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_airtable"],
    install_requires=[
        "backoff==1.3.2",
        "certifi==2018.11.29",
        "chardet==3.0.4",
        "idna==2.7",
        "pendulum==1.2.0",
        "python-dateutil>=2.7.5",
        "pytz==2018.4",
        "pytzdata==2018.7",
        "requests==2.26.0",
        "simplejson==3.11.1",
        "singer-python>=5.4.0",
        "python-slugify==4.0.1",
        "six==1.12.0",
        "tzlocal==1.5.1",
        "custom-logger @ git+https://github.com/olbycom/nekt-custom-logger-module.git@v0.0.7#egg=custom-logger",
    ],
    entry_points="""
          [console_scripts]
          tap-airtable=tap_airtable:main
      """,
    packages=find_packages(),
    include_package_data=True,
)
