import random

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

    # push data to gbq
    pd.DataFrame.to_gbq(
        df,
        destination_table="dataset_test.table_test",
        project_id="dask-bigquery",
        chunksize=5,
        if_exists="append",
    )
    yield "dask-bigquery.dataset_test.table_test"


# test simple read
def test_read_gbq(df, dataset, client):
    """Test simple read of data pushed to BigQuery using pandas-gbq"""
    project_id, dataset_id, table_id = dataset.split(".")

    ddf = read_gbq(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

    assert ddf.columns.tolist() == ["name", "number", "idx"]
    assert len(ddf) == 10
    assert ddf.npartitions == 2

    assert assert_eq(ddf.set_index("idx").compute(), df.set_index("idx"))


# test partitioned data: this test requires a copy of the public dataset
# bigquery-public-data.covid19_public_forecasts.county_14d into a the
# project dask-bigquery


@pytest.mark.parametrize(
    "fields",
    ([], ["county_name"], ["county_name", "county_fips_code"]),
    ids=["no_fields", "missing_partition_field", "fields"],
)
def test_read_gbq_partitioning(fields, client):
    partitions = ["Teton", "Loudoun"]
    ddf = read_gbq(
        project_id="dask-bigquery",
        dataset_id="covid19_public_forecasts",
        table_id="county_14d",
        partition_field="county_name",
        partitions=partitions,
        fields=fields,
    )

    assert len(ddf)  # check it's not empty
    loaded = set(ddf.columns) | {ddf.index.name}

    if fields:
        assert loaded == set(fields) | {"county_name"}
    else:  # all columns loaded
        assert loaded >= set(["county_name", "county_fips_code"])

    assert ddf.npartitions == len(partitions)
    assert list(ddf.divisions) == sorted(ddf.divisions)
