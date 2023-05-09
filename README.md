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

## Authentication

Default credentials can be provided by setting the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the file name:

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
import random
import pandas as pd
import dask.dataframe as dd
import dask_bigquery
from datetime import datetime, timedelta

df = pd.DataFrame({
    "name": [random.choice(["Carol", "Doug", "John", "Mark", "Susan", "Jing-Mei"]) for _ in range(10)],
    "number": [random.randint(0, 100) for _ in range(10)],
    "timestamp": [datetime.now() - timedelta(days=random.randint(0, 3)) for _ in range(10)]
})
ddf = dd.from_pandas(df, npartitions=2)
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

`to_gbq` writes intermediary Parquet to Google Storage bucket. Default bucket name is set to "dask-bigquery-tmp". You can provide a diferent bucket name by setting the parameter: `bucket="my-gs-bucket"`.

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
