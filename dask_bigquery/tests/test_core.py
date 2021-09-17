import random
import uuid

import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq
from distributed.utils_test import cluster_fixture  # noqa: F401
from distributed.utils_test import client, loop  # noqa: F401
from google.cloud import bigquery

from dask_bigquery import read_gbq

# These tests are run locally and assume the user is already athenticated.
# It also assumes that the user has created a project called dask-bigquery.


@pytest.fixture
def df():
    records = [
        {
            "name": random.choice(["fred", "wilma", "barney", "betty"]),
            "number": random.randint(0, 100),
            "idx": i,
        }
        for i in range(10)
    ]

    yield pd.DataFrame(records)


@pytest.fixture
def dataset(df):
    "Push some data to BigQuery using pandas gbq"
    project_id = "dask-bigquery"
    dataset_id = uuid.uuid4().hex
    table_id = "table_test"
    # push data to gbq
    pd.DataFrame.to_gbq(
        df,
        destination_table=f"{dataset_id}.{table_id}",
        project_id=project_id,
        chunksize=5,
        if_exists="append",
    )
    yield (project_id, dataset_id, table_id)

    with bigquery.Client() as bq_client:
        bq_client.delete_dataset(
            dataset=f"{project_id}.{dataset_id}",
            delete_contents=True,
        )


# test simple read
def test_read_gbq(df, dataset, client):
    """Test simple read of data pushed to BigQuery using pandas-gbq"""
    project_id, dataset_id, table_id = dataset
    ddf = read_gbq(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

    assert list(ddf.columns) == ["name", "number", "idx"]
    assert ddf.npartitions == 2
    assert assert_eq(ddf.set_index("idx"), df.set_index("idx"))
