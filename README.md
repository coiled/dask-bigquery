# Dask-BigQuery

[![Tests](https://github.com/coiled/dask-bigquery/actions/workflows/tests.yml/badge.svg)](https://github.com/coiled/dask-bigquery/actions/workflows/tests.yml) [![Linting](https://github.com/coiled/dask-bigquery/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/coiled/dask-bigquery/actions/workflows/pre-commit.yml)

Read data from Google BigQuery with Dask

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

## History

This project stems from the discussion in
[this Dask issue](https://github.com/dask/dask/issues/3121) and
[this initial implementation](https://gist.github.com/bnaul/4819f045ccbee160b60a530b6cfc0c98#file-dask_bigquery-py)
developed by [Brett Naul](https://github.com/bnaul), [Jacob Hayes](https://github.com/JacobHayes),
and [Steven Soojin Kim](https://github.com/mikss).

## License 

[BSD-3](LICENSE)