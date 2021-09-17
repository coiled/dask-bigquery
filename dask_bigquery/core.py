from __future__ import annotations

from contextlib import contextmanager
from functools import partial

import pandas as pd
import pyarrow
from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from google.cloud import bigquery, bigquery_storage


@contextmanager
def bigquery_client(project_id=None):
    """This context manager is a temporary solution until there is an
    upstream solution to handle this.
    See  googleapis/google-cloud-python#9457
    and googleapis/gapic-generator-python#575 for reference.
    """

    bq_storage_client = None
    with bigquery.Client(project_id) as bq_client:
        try:
            bq_storage_client = bigquery_storage.BigQueryReadClient(
                credentials=bq_client._credentials
            )
            yield bq_client, bq_storage_client
        finally:
            bq_storage_client.transport.grpc_channel.close()


def _stream_to_dfs(bqs_client, stream_name, schema, timeout):
    """Given a Storage API client and a stream name, yield all dataframes."""
    return [
        pyarrow.ipc.read_record_batch(
            pyarrow.py_buffer(message.arrow_record_batch.serialized_record_batch),
            schema,
        ).to_pandas()
        for message in bqs_client.read_rows(name=stream_name, offset=0, timeout=timeout)
    ]


def bigquery_read(
    make_create_read_session_request: callable,
    project_id: str,
    timeout: int,
    stream_name: str,
) -> pd.DataFrame:
    """Read a single batch of rows via BQ Storage API, in Arrow binary format.
    Args:
        project_id: BigQuery project
        create_read_session_request: kwargs to pass to `bqs_client.create_read_session`
        as `request`
        stream_name: BigQuery Storage API Stream "name".
            NOTE: Please set if reading from Storage API without any `row_restriction`.
            https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta1#stream
    NOTE: `partition_field` and `stream_name` kwargs are mutually exclusive.
    Adapted from
    https://github.com/googleapis/python-bigquery-storage/blob/a0fc0af5b4447ce8b50c365d4d081b9443b8490e/google/cloud/bigquery_storage_v1/reader.py.
    """
    with bigquery_client(project_id) as (bq_client, bqs_client):
        session = bqs_client.create_read_session(make_create_read_session_request())
        schema = pyarrow.ipc.read_schema(
            pyarrow.py_buffer(session.arrow_schema.serialized_schema)
        )
        shards = _stream_to_dfs(bqs_client, stream_name, schema, timeout=timeout)
        # NOTE: BQ Storage API can return empty streams
        if len(shards) == 0:
            shards = [schema.empty_table().to_pandas()]

    return pd.concat(shards)


def read_gbq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    row_filter="",
    read_timeout: int = 3600,
):
    """Read table as dask dataframe using BigQuery Storage API via Arrow format.
    If `partition_field` and `partitions` are specified, then the resulting dask dataframe
    will be partitioned along the same boundaries. Otherwise, partitions will be approximately
    balanced according to BigQuery stream allocation logic.
    If `partition_field` is specified but not included in `fields` (either implicitly by requesting
    all fields, or explicitly by inclusion in the list `fields`), then it will still be included
    in the query in order to have it available for dask dataframe indexing.
    Args:
        project_id: BigQuery project
        dataset_id: BigQuery dataset within project
        table_id: BigQuery table within dataset
        partition_field: to specify filters of form "WHERE {partition_field} = ..."
        partitions: all values to select of `partition_field`
        fields: names of the fields (columns) to select (default None to "SELECT *")
        read_timeout: # of seconds an individual read request has before timing out
    Returns:
        dask dataframe
    See https://github.com/dask/dask/issues/3121 for additional context.
    """

    with bigquery_client(project_id) as (
        bq_client,
        bqs_client,
    ):
        table_ref = bq_client.get_table(".".join((dataset_id, table_id)))
        if table_ref.table_type == "VIEW":
            raise TypeError("Table type VIEW not supported")

        # The protobuf types can't be pickled (may be able to tweak w/ copyreg), so instead use a
        # generator func.
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
            read_timeout,
        )

        layer = DataFrameIOLayer(
            output_name,
            meta.columns,
            [stream.name for stream in session.streams],
            partial(
                bigquery_read,
                make_create_read_session_request,
                project_id,
                read_timeout,
            ),
            label=label,
        )
        divisions = tuple([None] * (len(session.streams) + 1))

        graph = HighLevelGraph({output_name: layer}, {output_name: set()})
        return new_dd_object(graph, output_name, meta, divisions)
