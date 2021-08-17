import random

import pandas as pd
from distributed.utils_test import cluster_fixture  # noqa: F401
from distributed.utils_test import client, loop  # noqa: F401
from google.cloud import bigquery

from dask_bigquery import read_gbq

# These tests are run locally and assume the user is already athenticated.
# It also assumes that the user has created a project called dask-bigquery.


def gen_data(size=10):
    records = [
        {
            "name": random.choice(["fred", "wilma", "barney", "betty"]),
            "number": random.randint(0, 100),
            "idx": i,
        }
        for i in range(size)
    ]
    return pd.DataFrame(records)


def push_data():
    "Push data to BigQuery using pandas gbq"
    df = gen_data()

    pd.DataFrame.to_gbq(
        df,
        destination_table="dataset_test.table_test",
        project_id="dask-bigquery",
        chunksize=5,
        if_exists="append",
    )

    return df


def test_read_gbq(client):
    """Test simple read of data pushed to BigQuery using pandas-gbq"""
    try:
        # delete data set if exists
        bq_client = bigquery.Client()
        bq_client.delete_dataset(
            dataset="dask-bigquery.dataset_test",
            delete_contents=True,
        )
        bq_client.close()
    except:  # if data doesn't exisit continue is that Value Error?
        pass
    # create data
    df = push_data()

    ddf = read_gbq(
        project_id="dask-bigquery", dataset_id="dataset_test", table_id="table_test"
    )

    assert ddf.columns.tolist() == ["name", "number", "idx"]
    assert len(ddf) == 10
    assert ddf.npartitions == 2

    ddf_comp = ddf.set_index("idx").compute()
    # breakpoint()
    assert all(ddf_comp == df.set_index("idx"))
