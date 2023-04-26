import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import dask.dataframe as dd
import google.auth
import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq
from distributed.utils_test import cleanup  # noqa: F401
from distributed.utils_test import client  # noqa: F401
from distributed.utils_test import cluster_fixture  # noqa: F401
from distributed.utils_test import loop  # noqa: F401
from distributed.utils_test import loop_in_thread  # noqa: F401
from google.cloud import bigquery

from dask_bigquery import read_gbq, to_dbq


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


@pytest.fixture
def write_dataset():
    project_id = os.environ.get("DASK_BIGQUERY_PROJECT_ID")
    if not project_id:
        _, project_id = google.auth.default()
    dataset_id = uuid.uuid4().hex

    yield project_id, dataset_id

    with bigquery.Client() as bq_client:
        bq_client.delete_dataset(
            dataset=f"{project_id}.{dataset_id}",
            delete_contents=True,
        )


def test_to_dbq(df, write_dataset):
    pytest.importorskip("gcsfs", reason="Requires gcfs to write to Google Storage")
    project_id, dataset_id = write_dataset
    ddf = dd.from_pandas(df, npartitions=2)
    result = to_dbq(
        ddf,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id="table_to_write",
    )
    assert result.state == "DONE"


def test_read_gbq(df, dataset, client):
    project_id, dataset_id, table_id = dataset
    ddf = read_gbq(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

    assert list(ddf.columns) == ["name", "number", "timestamp", "idx"]
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


def test_max_streams(df, dataset, client):
    project_id, dataset_id, table_id = dataset
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        max_stream_count=1,
    )
    assert ddf.npartitions == 1
