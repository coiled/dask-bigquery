import os
import random
import uuid

import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq
from distributed.utils_test import cluster_fixture  # noqa: F401
from distributed.utils_test import client, loop  # noqa: F401
from google.cloud import bigquery

from dask_bigquery import read_gbq


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
    project_id = os.environ.get("DASK_BIGQUERY_PROJECT_ID", "dask-bigquery")
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


def test_read_gbq(df, dataset, client):
    project_id, dataset_id, table_id = dataset
    ddf = read_gbq(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

    assert list(ddf.columns) == ["name", "number", "idx"]
    assert ddf.npartitions == 2
    assert assert_eq(ddf.set_index("idx"), df.set_index("idx"))


def test_read_row_filter(df, dataset, client):
    project_id, dataset_id, table_id = dataset
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        row_filter="idx < 5",
    )

    assert list(ddf.columns) == ["name", "number", "idx"]
    assert ddf.npartitions == 2
    assert assert_eq(ddf.set_index("idx").loc[:4], df.set_index("idx").loc[:4])


def test_read_kwargs(dataset, client):
    project_id, dataset_id, table_id = dataset
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        read_kwargs={"timeout": 1e-12},
    )

    with pytest.raises(Exception, match="504 Deadline Exceeded"):
        ddf.compute()
