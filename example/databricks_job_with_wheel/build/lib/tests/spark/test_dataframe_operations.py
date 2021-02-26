from pyspark.sql.functions import col

from pipelines.config import paths, schemas
from pipelines.operations import (
    create_stream_writer,
    transform_bronze,
    transform_raw,
    prepare_interpolated_updates_dataframe,
    update_silver_table,
)


class TestSparkDataframeOperations:
    def test_create_stream_writer(self, spark, loaded_raw_df):
        transformed_raw_df = transform_raw(spark, loaded_raw_df)
        raw_to_bronze_writer = create_stream_writer(
            dataframe=transformed_raw_df,
            path=paths.bronze,
            checkpoint=paths.bronze_checkpoint,
            name="write_raw_to_bronze",
            partition_column="p_ingestdate",
        )
        assert raw_to_bronze_writer._df.schema == schemas.bronze

    def test_transform_raw(self, spark, sample_raw_df, sample_bronze_df):

        transformed_raw_df = transform_raw(spark, sample_raw_df)

        assert (
            transformed_raw_df.drop("ingesttime")
            .intersect(sample_bronze_df.drop("ingesttime"))
            .count()
            == transformed_raw_df.count()
        ) and (
            sample_bronze_df.drop("ingesttime")
            .intersect(transformed_raw_df.drop("ingesttime"))
            .count()
            == sample_bronze_df.count()
        )

    def test_transform_bronze(self, spark, sample_bronze_df, sample_silver_1_df):

        transformed_bronze_df = transform_bronze(spark, sample_bronze_df)

        assert (
            transformed_bronze_df.intersect(sample_silver_1_df).count()
            == transformed_bronze_df.count()
        ) and (
            sample_silver_1_df.intersect(transformed_bronze_df).count()
            == sample_silver_1_df.count()
        )

    def test_prepare_interpolated_updates_dataframe(self, spark, sample_silver_2_df):

        updates_df = prepare_interpolated_updates_dataframe(spark, sample_silver_2_df)
        assert updates_df.count() == 1
        assert updates_df.select("heartrate").collect().pop().heartrate == 61.9946573478

    def test_prepare_interpolated_updates_dataframe_boundary_case(
        self, spark, sample_silver_1_df
    ):

        updates_df = prepare_interpolated_updates_dataframe(spark, sample_silver_1_df)
        assert updates_df.count() == 1
        assert updates_df.select("heartrate").collect().pop().heartrate == 61.3702019913

    def test_update_silver_table(self, spark, full_silver_df):
        assert full_silver_df.where(col("heartrate") < 0).count() == 75
        update_silver_table(spark, paths.silver)
        assert full_silver_df.where(col("heartrate") < 0).count() == 0
