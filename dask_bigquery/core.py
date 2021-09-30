from __future__ import annotations

from contextlib import contextmanager
from functools import partial

import pandas as pd
import pyarrow
from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from google.api_core import client_info as rest_client_info
from google.api_core.gapic_v1 import client_info as grpc_client_info
from google.cloud import bigquery, bigquery_storage
from google.oauth2 import service_account

import dask_bigquery


@contextmanager
def bigquery_clients(project_id, cred_fpath):
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

    if cred_fpath:
        credentials = service_account.Credentials.from_service_account_file(cred_fpath)
    else:
        credentials = cred_fpath  # if no path set to None to try read default

    with bigquery.Client(
        project_id, credentials=credentials, client_info=bq_client_info
    ) as bq_client:
        bq_storage_client = bigquery_storage.BigQueryReadClient(
            credentials=bq_client._credentials,
            client_info=bqstorage_client_info,
        )
        yield bq_client, bq_storage_client
        bq_storage_client.transport.grpc_channel.close()


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
    cred_fpath: str,
    read_kwargs: dict,
    stream_name: str,
) -> pd.DataFrame:
    """Read a single batch of rows via BQ Storage API, in Arrow binary format.

    Parameters
    ----------
    create_read_session_request: callable
      kwargs to pass to `bqs_client.create_read_session` as `request`
    project_id: str
      Name of the BigQuery project.
    read_kwargs: dict
      kwargs to pass to read_rows()
    stream_name: str
      BigQuery Storage API Stream "name"
      NOTE: Please set if reading from Storage API without any `row_restriction`.
            https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta1#stream
    """
    with bigquery_clients(project_id, cred_fpath) as (_, bqs_client):
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
    row_filter="",
    *,
    cred_fpath: str = None,
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
    cred_fpath: str
        path for the service account key json file.
    read_kwargs: dict
      kwargs to pass to read_rows()

    Returns
    -------
        Dask DataFrame
    """
    read_kwargs = read_kwargs or {}
    with bigquery_clients(project_id, cred_fpath) as (bq_client, bqs_client):
        table_ref = bq_client.get_table(f"{dataset_id}.{table_id}")
        if table_ref.table_type == "VIEW":
            raise TypeError("Table type VIEW not supported")

        def make_create_read_session_request(row_filter=""):
            return bigquery_storage.types.CreateReadSessionRequest(
                max_stream_count=100,  # 0 -> use as many streams as BQ Storage will provide
                parent=f"projects/{project_id}",
                read_session=bigquery_storage.types.ReadSession(
                    data_format=bigquery_storage.types.DataFormat.ARROW,
                    read_options=bigquery_storage.types.ReadSession.TableReadOptions(
                        row_restriction=row_filter,
                    ),
                    table=table_ref.to_bqstorage(),
                ),
            )

        # Create a read session in order to detect the schema.
        # Read sessions are light weight and will be auto-deleted after 24 hours.
        session = bqs_client.create_read_session(
            make_create_read_session_request(row_filter=row_filter)
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
                cred_fpath,
                read_kwargs,
            ),
            label=label,
        )
        divisions = tuple([None] * (len(session.streams) + 1))

        graph = HighLevelGraph({output_name: layer}, {output_name: set()})
        return new_dd_object(graph, output_name, meta, divisions)
