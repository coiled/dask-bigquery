# Dask-BigQuery

[![Tests](https://github.com/coiled/dask-bigquery/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/dask-bigquery/actions/workflows/tests.yml) [![Linting](https://github.com/coiled/dask-bigquery/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/coiled/dask-bigquery/actions/workflows/pre-commit.yml)

Read/write data from/to [Google BigQuery](https://cloud.google.com/bigquery) with Dask.

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

## Google Cloud permissions

For reading from BiqQuery, you need the following roles to be enabled on the account:

- `BigQuery Read Session User`
- `BigQuery Data Viewer`, `BigQuery Data Editor`, or `BigQuery Data Owner`

Alternately, `BigQuery Admin` would give you full access to sessions and data.

For writing to BigQuery, the following roles are sufficient:

- `BigQuery Data Editor`
- `Storage Object Creator`

The minimal permissions to cover reading and writing:

- `BigQuery Data Editor`
- `BigQuery Read Session User`
- `Storage Object Creator`

## Authentication

By default, `dask-bigquery` will use the [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc). When running code locally, you can set this to use your user credentials by running

```sh
$ gcloud auth application-default login
```

User credentials require interactive login. For settings where this isn't possible, you'll need to create a service account. You can set the Application Default Credentials to the service account key using the `GOOGLE_APPLICATION_CREDENTIALS` environment variable:

```sh
$ export GOOGLE_APPLICATION_CREDENTIALS=/home/<username>/google.json
```

For information on obtaining the credentials, use [Google API documentation](https://developers.google.com/workspace/guides/create-credentials).

## Example: read from BigQuery

`dask-bigquery` assumes that you are already authenticated.

```python
import dask_bigquery

ddf = dask_bigquery.read_gbq(
    project_id="your_project_id",
    dataset_id="your_dataset",
    table_id="your_table",
)

ddf.head()
```

## Example: write to BigQuery

With default credentials:

```python
import dask
import dask_bigquery

ddf = dask.datasets.timeseries(freq="1min")

res = dask_bigquery.to_gbq(
    ddf,
    project_id="my_project_id",
    dataset_id="my_dataset_id",
    table_id="my_table_name",
)
```

With explicit credentials:

```python
from google.oauth2.service_account import Credentials

# credentials
creds_dict = {"type": ..., "project_id": ..., "private_key_id": ...}
credentials = Credentials.from_service_account_info(info=creds_dict)

res = to_gbq(
    ddf,
    project_id="my_project_id",
    dataset_id="my_dataset_id",
    table_id="my_table_name",
    credentials=credentials,
)
```

Before loading data into BigQuery, `to_gbq` writes intermediary Parquet to a Google Storage bucket. Default bucket name is `dask-bigquery-tmp`. You can provide a diferent bucket name by setting the parameter: `bucket="my-gs-bucket"`. After the job is done, the intermediary data is deleted.

If you're using a persistent bucket, we recommend configuring a retention policy that ensures the data is cleaned up even in case of job failures.

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
