import pytest
from datetime import datetime
from shutil import rmtree
from pyspark.sql import DataFrame, SparkSession
from pipelines.config import paths, schemas

from pipelines.utility import (
    generate_spark_session,
    load_dataframe,
    until_stream_is_ready,
)


def test_data_value(time: float, heartrate: float) -> str:
    value = f'{{"time":{time},"name":"Deborah Powell","device_id":0,'
    value += f'"device_type": "version 2","heartrate":{heartrate}}}'
    return value


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    yield generate_spark_session()


@pytest.fixture(scope="module")
def sample_bronze_df(spark: SparkSession) -> DataFrame:

    yield spark.createDataFrame(
        [
            {
                "datasource": "files.training.databricks.com",
                "ingesttime": datetime.now(),
                "value": test_data_value(1585699200.0, -62.320653259),
                "p_ingestdate": datetime.now(),
            },
            {
                "datasource": "files.training.databricks.com",
                "ingesttime": datetime.now(),
                "value": test_data_value(1585702800.0, 61.3702019913),
                "p_ingestdate": datetime.now(),
            },
        ],
        schema=schemas.bronze,
    )


@pytest.fixture(scope="module")
def sample_raw_df(spark: SparkSession) -> DataFrame:

    yield spark.createDataFrame(
        [
            {"value": test_data_value(1585699200.0, -62.320653259)},
            {"value": test_data_value(1585702800.0, 61.3702019913)},
        ]
    )


@pytest.fixture(scope="module")
def sample_silver_1_df(spark: SparkSession) -> DataFrame:

    yield spark.createDataFrame(
        [
            {
                "device_id": 0,
                "device_type": "version 2",
                "heartrate": -62.320653259,
                "eventtime": datetime(2020, 4, 1, 0, 0, 0),
                "name": "Deborah Powell",
                "p_eventdate": datetime(2020, 4, 1),
            },
            {
                "device_id": 0,
                "device_type": "version 2",
                "heartrate": 61.3702019913,
                "eventtime": datetime(2020, 4, 1, 0, 59, 44),
                "name": "Deborah Powell",
                "p_eventdate": datetime(2020, 4, 1),
            },
        ],
        schema=schemas.silver,
    )


@pytest.fixture(scope="module")
def sample_silver_2_df(spark: SparkSession) -> DataFrame:

    yield spark.createDataFrame(
        [
            {
                "device_id": 0,
                "device_type": "version 2",
                "heartrate": 62.320653259,
                "eventtime": "2020-04-01T00:00:00.000Z",
                "name": "Deborah Powell",
                "p_eventdate": "2020-04-01",
            },
            {
                "device_id": 0,
                "device_type": "version 2",
                "heartrate": -61.3702019913,
                "eventtime": "2020-04-01T00:59:44.000Z",
                "name": "Deborah Powell",
                "p_eventdate": "2020-04-01",
            },
            {
                "device_id": 0,
                "device_type": "version 2",
                "heartrate": 61.6686614366,
                "eventtime": "2020-04-01T01:59:28.000Z",
                "name": "Deborah Powell",
                "p_eventdate": "2020-04-01",
            },
        ]
    )


@pytest.fixture(scope="module")
def full_raw_df(spark: SparkSession) -> DataFrame:
    yield load_dataframe(
        spark, format="text", path=paths.test_raw, schema=schemas.raw, streaming=True
    )


@pytest.fixture(scope="module")
def full_bronze_df(spark: SparkSession) -> DataFrame:
    yield load_dataframe(
        spark,
        format="json",
        path=paths.test_bronze,
        schema=schemas.bronze,
        streaming=True,
    )


@pytest.fixture()
def loaded_raw_df(spark: SparkSession) -> DataFrame:
    yield load_dataframe(
        spark,
        format="text",
        path=paths.test_raw,
        schema=schemas.raw,
        streaming=True,
    )


@pytest.fixture()
def loaded_bronze_df(spark: SparkSession) -> DataFrame:
    yield load_dataframe(
        spark,
        format="delta",
        path=paths.bronze,
        schema=schemas.bronze,
        streaming=True,
    )
    rmtree(paths.bronze)
    rmtree(paths.bronze_checkpoint)


@pytest.fixture()
def loaded_silver_df(spark: SparkSession) -> DataFrame:
    yield load_dataframe(
        spark,
        format="delta",
        path=paths.silver,
        schema=schemas.silver,
        streaming=True,
    )
    rmtree(paths.silver)
    rmtree(paths.silver_checkpoint)


@pytest.fixture()
def full_silver_df(spark: SparkSession) -> DataFrame:
    stream_name = "create_silver"
    silver_json_df = load_dataframe(
        spark,
        format="json",
        path=paths.test_silver,
        schema=schemas.silver,
        streaming=True,
    )
    (
        silver_json_df.writeStream.format("delta")
        .partitionBy("p_eventdate")
        .outputMode("append")
        .option("checkpointLocation", paths.silver_checkpoint)
        .option("path", paths.silver)
        .queryName(stream_name)
        .start()
    )
    until_stream_is_ready(spark, stream_name)
    yield load_dataframe(spark, format="delta", path=paths.silver)
    rmtree(paths.silver)
    rmtree(paths.silver_checkpoint)
