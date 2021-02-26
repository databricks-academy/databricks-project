from pipelines.operations import create_stream_writer, transform_raw, transform_bronze


class TestSparkIntegrations:
    def test_raw_to_bronze(self, spark, full_raw_df, loaded_bronze_df):
        stream_name = "write_raw_to_bronze"
        transformed_raw_df = transform_raw(spark, full_raw_df)
        raw_to_bronze_writer = create_stream_writer(
            dataframe=transformed_raw_df,
            path=paths.bronze,
            checkpoint=paths.bronze_checkpoint,
            name=stream_name,
            partition_column="p_ingestdate",
        )
        raw_to_bronze_writer.start()

        until_stream_is_ready(spark, stream_name)
        assert loaded_bronze_df.count() == 7320

    def test_bronze_to_silver(self, spark, test_bronze_df, loaded_silver_df):
        stream_name = "write_bronze_to_silver"
        transformed_bronze_df = transform_bronze(spark, test_bronze_df)
        bronze_to_silver_writer = create_stream_writer(
            dataframe=transformed_bronze_df,
            path=paths.silver,
            checkpoint=paths.silver_checkpoint,
            name=stream_name,
            partition_column="p_eventdate",
        )
        bronze_to_silver_writer.start()

        until_stream_is_ready(spark, stream_name)
        assert loaded_silver_df.count() == 7320
