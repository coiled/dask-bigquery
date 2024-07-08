from __future__ import annotations

import datetime as dt
import warnings
from contextlib import contextmanager
from functools import partial

import gcsfs
import pandas as pd
import pyarrow
from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from google.api_core import client_info as rest_client_info
from google.api_core import exceptions
from google.api_core.gapic_v1 import client_info as grpc_client_info
from google.cloud import bigquery, bigquery_storage
from google.oauth2 import service_account

import dask_bigquery


@contextmanager
def bigquery_clients(project_id, credentials: dict = None):
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

    # Google library client needs an instance of google.auth.credentials.Credentials
    if isinstance(credentials, dict):
        credentials = service_account.Credentials.from_service_account_info(
            info=credentials
        )

    with bigquery.Client(
        project_id, credentials=credentials, client_info=bq_client_info
    ) as bq_client:
        bq_storage_client = bigquery_storage.BigQueryReadClient(
            credentials=bq_client._credentials,
            client_info=bqstorage_client_info,
        )
        yield bq_client, bq_storage_client
        bq_storage_client.transport.grpc_channel.close()


@contextmanager
def bigquery_client(project_id, credentials: dict = None):
    """Create the BigQuery Client"""
    client_info = rest_client_info.ClientInfo(
        user_agent=f"dask-bigquery/{dask_bigquery.__version__}"
    )

    # we allow to pass in a dict in to_gbq, but the Google library client needs
    # an instance of google.auth.credentials.Credentials
    if isinstance(credentials, dict):
        credentials = service_account.Credentials.from_service_account_info(
            info=credentials
        )

    with bigquery.Client(
        project_id, credentials=credentials, client_info=client_info
    ) as client:
        yield client


def gcs_fs(project_id, credentials: dict = None):
    """Create the GCSFS client"""
    return gcsfs.GCSFileSystem(
        project=project_id, access="read_write", token=credentials
    )


def _stream_to_dfs(bqs_client, stream_name, schema, read_kwargs, arrow_options):
    """Given a Storage API client and a stream name, yield all dataframes."""
    return [
        pyarrow.ipc.read_record_batch(
            pyarrow.py_buffer(message.arrow_record_batch.serialized_record_batch),
            schema,
        ).to_pandas(**arrow_options)
        for message in bqs_client.read_rows(name=stream_name, offset=0, **read_kwargs)
    ]


def bigquery_read(
    stream_name: str,
    make_create_read_session_request: callable,
    project_id: str,
    read_kwargs: dict,
    arrow_options: dict,
    credentials: dict = None,
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
    arrow_options: dict
      kwargs to pass to record_batch.to_pandas() when converting from pyarrow to pandas. See
      https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html#pyarrow.RecordBatch.to_pandas
      for possible values
    stream_name: str
      BigQuery Storage API Stream "name"
      NOTE: Please set if reading from Storage API without any `row_restriction`.
            https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta1#stream
    """
    with bigquery_clients(project_id, credentials=credentials) as (_, bqs_client):
        session = bqs_client.create_read_session(make_create_read_session_request())
        schema = pyarrow.ipc.read_schema(
            pyarrow.py_buffer(session.arrow_schema.serialized_schema)
        )
        shards = _stream_to_dfs(
            bqs_client, stream_name, schema, read_kwargs, arrow_options
        )
        # NOTE: BQ Storage API can return empty streams
        if len(shards) == 0:
            shards = [schema.empty_table().to_pandas(**arrow_options)]

    return pd.concat(shards)


def read_gbq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    row_filter: str = "",
    columns: list[str] = None,
    max_stream_count: int = 0,
    read_kwargs: dict = None,
    arrow_options: dict = None,
    credentials: dict = None,
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
    read_kwargs: dict
      kwargs to pass to read_rows()
    arrow_options: dict
        kwargs to pass to record_batch.to_pandas() when converting from pyarrow to pandas. See
        https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html#pyarrow.RecordBatch.to_pandas
        for possible values
    credentials : dict, optional
      Credentials for accessing Google APIs. Use this parameter to override
      default credentials. The dict should contain service account credentials in JSON format.

    Returns
    -------
        Dask DataFrame
    """
    read_kwargs = read_kwargs or {}
    arrow_options = arrow_options or {}
    with bigquery_clients(project_id, credentials=credentials) as (
        bq_client,
        bqs_client,
    ):
        table_ref = bq_client.get_table(f"{dataset_id}.{table_id}")
        if table_ref.table_type == "VIEW":
            raise TypeError("Table type VIEW not supported")

        def make_create_read_session_request():
            return bigquery_storage.types.CreateReadSessionRequest(
                max_stream_count=max_stream_count,  # 0 -> use as many as BQ Storage will provide
                parent=f"projects/{project_id}",
                read_session=bigquery_storage.types.ReadSession(
                    data_format=bigquery_storage.types.DataFormat.ARROW,
                    read_options=bigquery_storage.types.ReadSession.TableReadOptions(
                        row_restriction=row_filter, selected_fields=columns
                    ),
                    table=table_ref.to_bqstorage(),
                ),
            )

        # Create a read session in order to detect the schema.
        # Read sessions are light weight and will be auto-deleted after 24 hours.
        session = bqs_client.create_read_session(make_create_read_session_request())
        schema = pyarrow.ipc.read_schema(
            pyarrow.py_buffer(session.arrow_schema.serialized_schema)
        )
        meta = schema.empty_table().to_pandas(**arrow_options)

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
                make_create_read_session_request=make_create_read_session_request,
                project_id=project_id,
                read_kwargs=read_kwargs,
                arrow_options=arrow_options,
                credentials=credentials,
            ),
            label=label,
        )
        divisions = tuple([None] * (len(session.streams) + 1))

        graph = HighLevelGraph({output_name: layer}, {output_name: set()})
        return new_dd_object(graph, output_name, meta, divisions)


def to_gbq(
    df,
    *,
    project_id: str,
    dataset_id: str,
    table_id: str,
    bucket: str = None,
    credentials: dict = None,
    delete_bucket: bool = False,
    parquet_kwargs: dict = None,
    load_job_kwargs: dict = None,
):
    """Write dask dataframe as table using BigQuery LoadJob.

    This method uses Parquet in GCS as intermediary storage, and requires GCS permission.

    Parameters
    ----------
    project_id: str
      Name of the BigQuery project id. If service account credentials are specified, and this
      parameter is None, project_id will be taken from credentials.
    dataset_id: str
      BigQuery dataset within project
    table_id: str
      BigQuery table within dataset
    bucket: str, default: {project_id}-dask-bigquery
      Google Cloud Storage bucket name, for intermediary parquet storage. If the bucket doesn't
      exist, it will be created. The account you're using will need Google Cloud Storage
      permissions (storage.objects.create, storage.buckets.create). If a persistent bucket is used,
      it is recommended to configure a retention policy that ensures the data is cleaned up in
      case of job failures.
    credentials : dict, optional
      Credentials for accessing Google APIs. Use this parameter to override
      default credentials. The dict should contain service account credentials in JSON format.
    delete_bucket: bool, default: False
      Delete bucket in GCS after loading intermediary data to Big Query. The bucket will only be deleted if it
      didn't exist before.
    parquet_kwargs: dict, default: {"write_index": False}
      Additional kwargs to pass to dataframe.write_parquet, such as schema, partition_on or
      write_index. For writing parquet, pyarrow is required. "engine" will always be set to "pyarrow", and
      "write_metadata_file" to False, even if different values are passed.
    load_job_kwargs: dict, default: {"write_disposition": "WRITE_EMPTY"}
      Additional kwargs to pass when creating bigquery.LoadJobConfig, such as schema,
      time_partitioning, clustering_fields, etc. If "schema" is passed, "autodetect" will be
      set to "False", otherwise "True".

    Returns
    -------
        LoadJobResult
    """
    if project_id is None and credentials:
        # service account credentials have a project associated with them
        project_id = credentials.get("project_id")

    load_job_kwargs_used = {"write_disposition": "WRITE_EMPTY"}
    if load_job_kwargs:
        load_job_kwargs_used.update(load_job_kwargs)

    load_job_kwargs_used["autodetect"] = "schema" not in load_job_kwargs_used

    # override the following kwargs, even if user specified them
    load_job_kwargs_used["source_format"] = bigquery.SourceFormat.PARQUET

    if bucket is None:
        bucket = f"{project_id}-dask-bigquery"

    fs = gcs_fs(project_id, credentials=credentials)
    if fs.exists(bucket):
        if delete_bucket:
            # if we didn't create it, we shouldn't delete it
            warnings.warn(
                "`delete_bucket=True` can only be used with a non-existing bucket.",
                category=UserWarning,
                stacklevel=2,
            )
            delete_bucket = False
    else:
        fs.mkdir(bucket)

    token = tokenize(df)
    object_prefix = f"{dt.datetime.utcnow().isoformat()}_{token}"
    path = f"gs://{bucket}/{object_prefix}"

    parquet_kwargs_used = {"write_index": False}
    if parquet_kwargs:
        parquet_kwargs_used.update(parquet_kwargs)

    # override the following kwargs, even if user specified them
    parquet_kwargs_used["engine"] = "pyarrow"
    parquet_kwargs_used["write_metadata_file"] = False
    parquet_kwargs_used.pop("path", None)
    if credentials is not None:
        if "storage_options" not in parquet_kwargs_used:
            parquet_kwargs_used["storage_options"] = {}
        parquet_kwargs_used["storage_options"]["token"] = credentials

    try:
        df.to_parquet(path, **parquet_kwargs_used)

        with bigquery_client(project_id, credentials=credentials) as client:
            # first try getting the dataset, in case user has read but not create
            # permissions. If it doesn't exist, we must create.
            try:
                target_dataset = client.get_dataset(dataset_id)
            except exceptions.NotFound:
                target_dataset = client.create_dataset(dataset_id)
            target_table = target_dataset.table(table_id)

            job_config = bigquery.LoadJobConfig(
                **load_job_kwargs_used,
            )
            job = client.load_table_from_uri(
                source_uris=f"{path}/*.parquet",
                destination=target_table,
                job_config=job_config,
            )

        return job.result()
    finally:
        # cleanup temporary parquet
        fs.rm(f"{bucket}/{object_prefix}", recursive=True)
        if delete_bucket:
            fs.rmdir(bucket)
