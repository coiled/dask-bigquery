# Dask-BigQuery

[![Tests](https://github.com/coiled/dask-bigquery/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/dask-bigquery/actions/workflows/tests.yml) [![Linting](https://github.com/coiled/dask-bigquery/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/coiled/dask-bigquery/actions/workflows/pre-commit.yml)

Read data from [Google BigQuery](https://cloud.google.com/bigquery) with Dask.

This package uses the BigQuery Storage API. Please refer to the [data extraction pricing table](https://cloud.google.com/bigquery/pricing#data_extraction_pricing) for associated costs while using Dask-BigQuery.

## Installation

`dask-bigquery` can be installed with `pip`:

```
pip install dask-bigquery
```

or with `conda`:

```
conda install -c conda-forge dask-bigquery
```

## Example

`dask-bigquery` assumes that you are already authenticated.

### Reading from BigQuery
```python
import dask_bigquery

ddf = dask_bigquery.read_gbq(
    project_id="your_project_id",
    dataset_id="your_dataset",
    table_id="your_table",
)

ddf.head()
```

### Writing to BigQuery
Uploads to BigQuery are performed by writing the data in Parquet format to an intermediate storage
bucket, then using a BigQuery load job to ingest the data into a table.
```python
import dask
import dask_bigquery

ddf = dask.datasets.timeseries(freq="1min")
dask_bigquery.to_gbq(
    ddf,
    project_id="your_project_id",
    dataset_id="your_dataset",
    table_id="your_table",
    bucket="your_bucket",  # bucket will be created if it doesn't exist
)


## Run tests locally

To run the tests locally you need to be authenticated and have a project created on that account. If you're using a service account, when created you need to select the role of "BigQuery Admin" in the section "Grant this service account access to project".

You can run the tests with

`$ pytest dask_bigquery`

if your default `gcloud` project is set, or manually specify the project ID with

`DASK_BIGQUERY_PROJECT_ID pytest dask_bigquery`

## History

This project stems from the discussion in
[this Dask issue](https://github.com/dask/dask/issues/3121) and
[this initial implementation](https://gist.github.com/bnaul/4819f045ccbee160b60a530b6cfc0c98#file-dask_bigquery-py)
developed by [Brett Naul](https://github.com/bnaul), [Jacob Hayes](https://github.com/JacobHayes),
and [Steven Soojin Kim](https://github.com/mikss).

## License 

[BSD-3](LICENSE)
