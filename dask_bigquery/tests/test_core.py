import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone

import dask
import dask.dataframe as dd
import gcsfs
import google.auth
import pandas as pd
import pyarrow as pa
import pytest
from dask.dataframe.utils import assert_eq, pyarrow_strings_enabled
from distributed.utils_test import cleanup  # noqa: F401
from distributed.utils_test import client  # noqa: F401
from distributed.utils_test import cluster_fixture  # noqa: F401
from distributed.utils_test import loop  # noqa: F401
from distributed.utils_test import loop_in_thread  # noqa: F401
from google.api_core.exceptions import InvalidArgument
from google.cloud import bigquery

from dask_bigquery import read_gbq, to_gbq


@pytest.fixture(scope="module")
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

    df = pd.DataFrame(records)
    df["timestamp"] = df["timestamp"].astype("datetime64[us, UTC]")
    yield df


@pytest.fixture(scope="module")
def dataset(project_id):
    dataset_id = f"{sys.platform}_{uuid.uuid4().hex}"

    with bigquery.Client() as bq_client:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        bq_client.create_dataset(dataset)

        yield dataset

        bq_client.delete_dataset(dataset=dataset, delete_contents=True)


@pytest.fixture(scope="module")
def table(dataset, df):
    project_id = dataset.project
    dataset_id = dataset.dataset_id
    table_id = "table_test"

    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp",
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", time_partitioning=time_partitioning
    )

    with bigquery.Client() as bq_client:
        job = bq_client.load_table_from_dataframe(
            df,
            destination=f"{project_id}.{dataset_id}.{table_id}",
            job_config=job_config,
        )  # Make an API request.
        job.result()

    yield project_id, dataset_id, table_id


@pytest.fixture(scope="module")
def required_partition_filter_table(dataset, df):
    project_id = dataset.project
    dataset_id = dataset.dataset_id
    table_id = "partitioned_table_test"

    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp",
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=time_partitioning,
    )

    with bigquery.Client() as bq_client:
        job = bq_client.load_table_from_dataframe(
            df,
            destination=f"{project_id}.{dataset_id}.{table_id}",
            job_config=job_config,
        )  # Make an API request.
        job.result()
        table = bq_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        table.require_partition_filter = True
        bq_client.update_table(table, ["require_partition_filter"])

    yield project_id, dataset_id, table_id


@pytest.fixture(scope="module")
def project_id():
    project_id = os.environ.get("DASK_BIGQUERY_PROJECT_ID")
    if not project_id:
        _, project_id = google.auth.default()

    yield project_id


@pytest.fixture
def google_creds():
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        credentials = json.load(open(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")))
    elif os.environ.get("DASK_BIGQUERY_GCP_CREDENTIALS"):
        credentials = json.loads(os.environ.get("DASK_BIGQUERY_GCP_CREDENTIALS"))
    else:
        credentials, _ = google.auth.default()

    yield credentials


@pytest.fixture
def bucket(google_creds, project_id):
    bucket = f"dask-bigquery-tmp-{uuid.uuid4().hex}"
    fs = gcsfs.GCSFileSystem(
        project=project_id, access="read_write", token=google_creds
    )

    yield bucket, fs

    if fs.exists(bucket):
        fs.rm(bucket, recursive=True)


@pytest.fixture
def write_dataset(google_creds, project_id):
    dataset_id = f"{sys.platform}_{uuid.uuid4().hex}"

    yield google_creds, project_id, dataset_id, None

    with bigquery.Client() as bq_client:
        bq_client.delete_dataset(
            dataset=f"{project_id}.{dataset_id}",
            delete_contents=True,
        )


@pytest.fixture
def write_existing_dataset(google_creds, project_id):
    dataset_id = "persistent_dataset"
    table_id = f"table_to_write_{sys.platform}_{uuid.uuid4().hex}"

    yield google_creds, project_id, dataset_id, table_id

    with bigquery.Client() as bq_client:
        bq_client.delete_table(
            f"{project_id}.{dataset_id}.{table_id}",
        )


@pytest.mark.parametrize(
    "parquet_kwargs,load_job_kwargs",
    [
        (None, None),
        (
            {
                "schema": pa.schema(
                    [
                        ("name", pa.string()),
                        ("number", pa.uint8()),
                        ("timestamp", pa.timestamp("us")),
                        ("idx", pa.uint8()),
                    ]
                ),
                "write_index": False,
            },
            {
                "schema": [
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("number", "INTEGER"),
                    bigquery.SchemaField("timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("idx", "INTEGER"),
                ],
                "autodetect": False,
            },
        ),
        ({"write_index": True}, None),  # non-default
        ({"engine": "fastparquet"}, None),  # non-default, should enforce "pyarrow"
        (None, {"write_disposition": "WRITE_APPEND"}),  # non-default
        (
            None,
            {
                "source_format": bigquery.SourceFormat.AVRO
            },  # non-default, should enforce PARQUET
        ),
    ],
)
@pytest.mark.parametrize("dataset_fixture", ["write_dataset", "write_existing_dataset"])
def test_to_gbq(df, dataset_fixture, request, parquet_kwargs, load_job_kwargs):
    dataset = request.getfixturevalue(dataset_fixture)

    _, project_id, dataset_id, table_id = dataset
    ddf = dd.from_pandas(df, npartitions=2)

    result = to_gbq(
        ddf,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id or "table_to_write",
        parquet_kwargs=parquet_kwargs,
        load_job_kwargs=load_job_kwargs,
    )
    assert result.state == "DONE"


@pytest.mark.parametrize("delete_bucket", [False, True])
@pytest.mark.parametrize("dataset_fixture", ["write_dataset", "write_existing_dataset"])
def test_to_gbq_cleanup(df, dataset_fixture, request, bucket, delete_bucket):
    dataset = request.getfixturevalue(dataset_fixture)
    _, project_id, dataset_id, table_id = dataset
    bucket, fs = bucket

    ddf = dd.from_pandas(df, npartitions=2)

    result = to_gbq(
        ddf,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id or "table_to_write",
        bucket=bucket,
        delete_bucket=delete_bucket,
    )
    assert result.state == "DONE"
    if delete_bucket:
        assert not fs.exists(bucket)
    else:
        # bucket should be empty
        assert fs.exists(bucket)
        assert len(fs.ls(bucket, detail=False)) == 0


@pytest.mark.parametrize("dataset_fixture", ["write_dataset", "write_existing_dataset"])
def test_to_gbq_with_credentials(df, dataset_fixture, request, monkeypatch):
    dataset = request.getfixturevalue(dataset_fixture)
    credentials, project_id, dataset_id, table_id = dataset
    ddf = dd.from_pandas(df, npartitions=2)

    monkeypatch.delenv("GOOGLE_DEFAULT_CREDENTIALS", raising=False)
    # with explicit credentials
    result = to_gbq(
        ddf,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id or "table_to_write",
        credentials=credentials,
    )
    assert result.state == "DONE"


@pytest.mark.parametrize("dataset_fixture", ["write_dataset", "write_existing_dataset"])
def test_roundtrip(df, dataset_fixture, request):
    dataset = request.getfixturevalue(dataset_fixture)
    _, project_id, dataset_id, table_id = dataset
    ddf = dd.from_pandas(df, npartitions=2)
    if not table_id:
        table_id = "roundtrip_table"

    result = to_gbq(
        ddf, project_id=project_id, dataset_id=dataset_id, table_id=table_id
    )
    assert result.state == "DONE"

    ddf_out = read_gbq(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
    # bigquery does not guarantee ordering, so let's reindex
    assert_eq(ddf.set_index("idx"), ddf_out.set_index("idx"), check_divisions=False)


def test_read_gbq(df, table, client):
    project_id, dataset_id, table_id = table
    ddf = read_gbq(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

    assert list(df.columns) == list(ddf.columns)
    assert assert_eq(ddf.set_index("idx"), df.set_index("idx"))


def test_read_row_filter(df, table, client):
    project_id, dataset_id, table_id = table
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        row_filter="idx < 5",
    )

    assert list(df.columns) == list(ddf.columns)
    assert assert_eq(ddf.set_index("idx").loc[:4], df.set_index("idx").loc[:4])


def test_read_kwargs(table, client):
    project_id, dataset_id, table_id = table
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        read_kwargs={"timeout": 1e-12},
    )

    with pytest.raises(Exception, match="Deadline"):
        ddf.compute()


def test_read_columns(df, table, client):
    project_id, dataset_id, table_id = table
    assert df.shape[1] > 1, "Test data should have multiple columns"

    columns = ["name"]
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        columns=columns,
    )
    assert list(ddf.columns) == columns


@pytest.mark.parametrize("dataset_fixture", ["write_dataset", "write_existing_dataset"])
def test_read_gbq_credentials(df, dataset_fixture, request, monkeypatch):
    dataset = request.getfixturevalue(dataset_fixture)
    credentials, project_id, dataset_id, table_id = dataset
    ddf = dd.from_pandas(df, npartitions=2)

    monkeypatch.delenv("GOOGLE_DEFAULT_CREDENTIALS", raising=False)
    # with explicit credentials
    result = to_gbq(
        ddf,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id or "table_to_write",
        credentials=credentials,
    )
    assert result.state == "DONE"

    # with explicit credentials
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id or "table_to_write",
        credentials=credentials,
    )

    assert list(df.columns) == list(ddf.columns)
    assert assert_eq(ddf.set_index("idx"), df.set_index("idx"))


def test_max_streams(df, table, client):
    project_id, dataset_id, table_id = table
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        max_stream_count=1,
    )
    assert ddf.npartitions == 1


def test_arrow_options(table):
    project_id, dataset_id, table_id = table
    ddf = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        arrow_options={"types_mapper": {pa.int64(): pd.Float32Dtype()}.get},
    )
    assert ddf.dtypes["number"] == pd.Float32Dtype()


@pytest.mark.parametrize("convert_string", [True, False, None])
def test_convert_string(table, convert_string, df):
    project_id, dataset_id, table_id = table
    config = {}
    if convert_string is not None:
        config = {"dataframe.convert-string": convert_string}
    with dask.config.set(config):
        ddf = read_gbq(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )
        # Roundtrip through `dd.from_pandas` to check consistent
        # behavior with Dask DataFrame
        result = dd.from_pandas(df, npartitions=1)
    if convert_string is True or (convert_string is None and pyarrow_strings_enabled()):
        assert ddf.dtypes["name"] == pd.StringDtype(storage="pyarrow")
    else:
        assert ddf.dtypes["name"] == object

    assert assert_eq(ddf.set_index("idx"), result.set_index("idx"))


@pytest.mark.skipif(sys.platform == "darwin", reason="Segfaults on macOS")
def test_read_required_partition_filter(df, required_partition_filter_table):
    project_id, dataset_id, table_id = required_partition_filter_table

    with pytest.raises(InvalidArgument):
        read_gbq(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        ).head()

    df = read_gbq(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        row_filter=f"DATE(timestamp)='{df.timestamp.min().date()}'",
    ).head()
    assert not df.empty
