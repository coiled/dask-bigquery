from __future__ import annotations

from contextlib import contextmanager
from functools import partial

import datetime as dt
import pandas as pd
import pyarrow
from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from google.api_core import client_info as rest_client_info
from google.api_core.gapic_v1 import client_info as grpc_client_info
from google.cloud import bigquery, bigquery_storage, storage

import dask_bigquery


@contextmanager
def bigquery_clients(project_id):
    """This context manager is a temporary solution until there is an
    upstream solution to handle this.
    See googleapis/google-cloud-python#9457
    and googleapis/gapic-generator-python#575 for reference.
    """
    bq_client_info = rest_client_info.ClientInfo(
        user_agent=f"dask-bigquery/{dask_bigquery.__version__}"
    )
    bqstorage_client_info = grpc_client_info.ClientInfo(
        user_agent=f"dask-bigquery/{dask_bigquery.__version__}"
    )

    with bigquery.Client(project_id, client_info=bq_client_info) as bq_client:
        bq_storage_client = bigquery_storage.BigQueryReadClient(
            credentials=bq_client._credentials,
            client_info=bqstorage_client_info,
        )
        yield bq_client, bq_storage_client
        bq_storage_client.transport.grpc_channel.close()


@contextmanager
def bigquery_client(project_id):
    """Create the BugQuery Client"""
    client_info = rest_client_info.ClientInfo(
        user_agent=f"dask-bigquery/{dask_bigquery.__version__}"
    )
    with bigquery.Client(project_id, client_info=client_info) as client:
        yield client


def gcs_client(project_id):
    """Create the Google Storage Client"""
    client_info = rest_client_info.ClientInfo(
        user_agent=f"dask-bigquery/{dask_bigquery.__version__}"
    )
    return storage.Client(project_id, client_info=client_info)


def _stream_to_dfs(bqs_client, stream_name, schema, read_kwargs):
    """Given a Storage API client and a stream name, yield all dataframes."""
    return [
        pyarrow.ipc.read_record_batch(
            pyarrow.py_buffer(message.arrow_record_batch.serialized_record_batch),
            schema,
        ).to_pandas()
        for message in bqs_client.read_rows(name=stream_name, offset=0, **read_kwargs)
    ]


def bigquery_read(
    make_create_read_session_request: callable,
    project_id: str,
    read_kwargs: dict,
    stream_name: str,
) -> pd.DataFrame:
    """Read a single batch of rows via BQ Storage API, in Arrow binary format.

    Parameters
    ----------
    make_create_read_session_request: callable
      callable that returns input args/kwargs for `bqs_client.create_read_session`
    project_id: str
      Name of the BigQuery project.
    read_kwargs: dict
      kwargs to pass to read_rows()
    stream_name: str
      BigQuery Storage API Stream "name"
      NOTE: Please set if reading from Storage API without any `row_restriction`.
            https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta1#stream
    """
    with bigquery_clients(project_id) as (_, bqs_client):
        session = bqs_client.create_read_session(make_create_read_session_request())
        schema = pyarrow.ipc.read_schema(
            pyarrow.py_buffer(session.arrow_schema.serialized_schema)
        )
        shards = _stream_to_dfs(bqs_client, stream_name, schema, read_kwargs)
        # NOTE: BQ Storage API can return empty streams
        if len(shards) == 0:
            shards = [schema.empty_table().to_pandas()]

    return pd.concat(shards)


def read_gbq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    row_filter: str = "",
    columns: list[str] = None,
    max_stream_count: int = 0,  # 0 -> use as many as BQ Storage will provide
    preferred_min_stream_count: int = 2,
    read_kwargs: dict = None,
):
    """Read table as dask dataframe using BigQuery Storage API via Arrow format.
    Partitions will be approximately balanced according to BigQuery stream allocation logic.

    Parameters
    ----------
    project_id: str
      Name of the BigQuery project id.
    dataset_id: str
      BigQuery dataset within project
    table_id: str
      BigQuery table within dataset
    row_filter: str
      SQL text filtering statement to pass to `row_restriction`
    columns: list[str]
      List of columns to load from the table
    max_stream_count: int
      Maximum number of streams to request from BQ storage; a value of 0 will request as many
      streams as possible. Note that BQ may return fewer streams than requested.
    preferred_min_stream_count: int
      The minimum preferred stream count. This parameter can be used to inform the service that
      there is a desired lower bound on the number of streams. This is typically a target parallelism
      of the client (e.g. a Spark cluster with N-workers would set this to a low multiple of N to
      ensure good cluster utilization). The system will make a best effort to provide at least this
      number of streams, but in some cases might provide less.
    read_kwargs: dict
      kwargs to pass to read_rows()

    Returns
    -------
        Dask DataFrame
    """
    read_kwargs = read_kwargs or {}
    with bigquery_clients(project_id) as (bq_client, bqs_client):
        table_ref = bq_client.get_table(f"{dataset_id}.{table_id}")
        if table_ref.table_type == "VIEW":
            raise TypeError("Table type VIEW not supported")

        def make_create_read_session_request(row_filter="", **request_kwargs):
            return bigquery_storage.types.CreateReadSessionRequest(
                parent=f"projects/{project_id}",
                read_session=bigquery_storage.types.ReadSession(
                    data_format=bigquery_storage.types.DataFormat.ARROW,
                    read_options=bigquery_storage.types.ReadSession.TableReadOptions(
                        row_restriction=row_filter, selected_fields=columns
                    ),
                    table=table_ref.to_bqstorage(),
                ),
                **request_kwargs,
            )

        # Create a read session in order to detect the schema.
        # Read sessions are light weight and will be auto-deleted after 24 hours.
        session = bqs_client.create_read_session(
            make_create_read_session_request(
                row_filter=row_filter,
                max_stream_count=max_stream_count,
                preferred_min_stream_count=preferred_min_stream_count,
            )
        )
        schema = pyarrow.ipc.read_schema(
            pyarrow.py_buffer(session.arrow_schema.serialized_schema)
        )
        meta = schema.empty_table().to_pandas()

        label = "read-gbq-"
        output_name = label + tokenize(
            project_id,
            dataset_id,
            table_id,
            row_filter,
            read_kwargs,
        )

        layer = DataFrameIOLayer(
            output_name,
            meta.columns,
            [stream.name for stream in session.streams],
            partial(
                bigquery_read,
                make_create_read_session_request,
                project_id,
                read_kwargs,
            ),
            label=label,
        )
        divisions = tuple([None] * (len(session.streams) + 1))

        graph = HighLevelGraph({output_name: layer}, {output_name: set()})
        return new_dd_object(graph, output_name, meta, divisions)


def to_dbq(
    df,
    *,
    project_id: str,
    dataset_id: str,
    table_id: str,
    gs_bucket: str = "dask-bigquery-tmp",
):
    """Write dask dataframe as table using BigQuery Load API.
    Writes Parquet to GCS for intermediary storage.

    Parameters
    ----------
    project_id: str
      Name of the BigQuery project id.
    dataset_id: str
      BigQuery dataset within project
    table_id: str
      BigQuery table within dataset
    gs_bucket: str
      Google Cloud Storage bucket name, for intermediary parquet storage. Will be created if
      not exists. Default: dask-bigquery-tmp.

    Returns
    -------
        LoadJobResult
    """
    if not project_id:
        raise ValueError("Required: project_id")
    if not table_id:
        raise ValueError("Required: table_id")
    if not dataset_id:
        raise ValueError("Required: dataset_id")

    storage_client = gcs_client(project_id)
    bucket = storage_client.lookup_bucket(bucket_name=gs_bucket)
    if not bucket:
        bucket = storage_client.create_bucket(gs_bucket)

    token = tokenize(df)
    object_prefix = f"{dt.datetime.utcnow():%Y-%m-%dT%H-%M-%S}_{token}"

    path = f"gs://{gs_bucket}/{object_prefix}"
    df.to_parquet(path=path, engine="pyarrow", write_metadata_file=False)

    try:
        with bigquery_client(project_id) as client:
            target_dataset = client.create_dataset(dataset_id, exists_ok=True)
            target_table = target_dataset.table(table_id)

            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_EMPTY",
            )
            job = bigquery.Client(project_id).load_table_from_uri(
                source_uris=f"{path}/*.parquet",
                destination=target_table,
                job_config=job_config,
            )

        return job.result()
    except Exception:
        raise
    finally:
        # cleanup temporary parquet
        for blob in bucket.list_blobs(prefix=object_prefix):
            blob.delete()
