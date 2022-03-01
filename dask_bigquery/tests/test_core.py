import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import google.auth
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
            "timestamp": datetime.now(timezone.utc) - timedelta(days=i % 2),
            "idx": i,
        }
        for i in range(10)
    ]

    yield pd.DataFrame(records)


@pytest.fixture
def dataset(df):
    project_id = os.environ.get("DASK_BIGQUERY_PROJECT_ID")
    if not project_id:
        credentials, project_id = google.auth.default()
    dataset_id = uuid.uuid4().hex
    table_id = "table_test"
    # push data to gbq

    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp",
    )  # field to use for partitioning

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", time_partitioning=time_partitioning
    )

    with bigquery.Client() as bq_client:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        bq_client.create_dataset(dataset)
        job = bq_client.load_table_from_dataframe(
            df,
            destination=f"{project_id}.{dataset_id}.{table_id}",
            job_config=job_config,
        )  # Make an API request.
        job.result()

    yield (project_id, dataset_id, table_id)

    with bigquery.Client() as bq_client:
        bq_client.delete_dataset(
            dataset=f"{project_id}.{dataset_id}",
            delete_contents=True,
        )


def test_read_gbq(df, dataset, client):
    project_id, dataset_id, table_id = dataset
    ddf = read_gbq(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

    assert list(ddf.columns) == ["name", "number", "timestamp", "idx"]
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

    assert list(ddf.columns) == ["name", "number", "timestamp", "idx"]
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

    with pytest.raises(Exception, match="Deadline"):
        ddf.compute()


def test_read_columns(df, dataset, client):
    project_id, dataset_id, table_id = dataset
    assert df.shape[1] > 1, "Test data should have multiple columns"

    columns = ["name"]
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        columns=columns,
    )
    assert list(ddf.columns) == columns
