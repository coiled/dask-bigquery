# Dask-BigQuery

[![Tests](https://github.com/coiled/dask-bigquery/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/dask-bigquery/actions/workflows/tests.yml) [![Linting](https://github.com/coiled/dask-bigquery/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/coiled/dask-bigquery/actions/workflows/pre-commit.yml)

Read data from Google BigQuery with Dask

**Note:** This project was based on the contributions from @bnaul, @JacobHayes and @mikss. The intial inspiration can be found in a [dask_bigquery gist](https://gist.github.com/bnaul/4819f045ccbee160b60a530b6cfc0c98#file-dask_bigquery-py)

## Installation


## Example

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

## License 

[BSD-3](LICENSE)