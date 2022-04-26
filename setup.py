#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dask-bigquery",
    version="2021.10.1",
    description="Dask + BigQuery intergration",
    license="BSD",
    packages=["dask_bigquery"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={
        "test": ["pytest", "pandas-gbq<=0.15", "distributed", "google-auth>=1.30.0"]
    },
    include_package_data=True,
    zip_safe=False,
)
