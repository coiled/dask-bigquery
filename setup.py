#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="dask-bigquery",
    version="2024.7.0",
    description="Dask + BigQuery integration",
    license="BSD",
    packages=["dask_bigquery"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    extras_require={"test": ["pytest", "distributed", "google-auth>=1.30.0"]},
    include_package_data=True,
    zip_safe=False,
)
