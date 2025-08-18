import pyspark.sql.functions as F
class DataTransforms:
    """
    Class containing data transformation functions for Streamify project.
    """

    @staticmethod
    def transform_data(spark, raw_df):
        """
        Example transformation function that processes input data.
        
        Args:
            data (any): Input data to be transformed.
        
        Returns
            any: Transformed data.
        """
        raw_df = raw_df.withColumn("eventTime", F.to_timestamp(raw_df["eventTime"]))
        raw_df = raw_df.withColumn("data",F.explode(raw_df["data.devices"]))
        raw_df = raw_df.withColumn("deviceId", F.col("data.deviceId"))
        raw_df = raw_df.withColumn("measure", F.col("data.measure"))
        raw_df = raw_df.withColumn("status", F.col("data.status"))
        raw_df = raw_df.withColumn("temperature", F.col("data.temperature"))
        raw_df = raw_df.drop("data", "devices")  # Drop the original '
        # raw_df = raw_df.withColumn('S_no',F.monotonically_increasing_id())  # Add a unique identifier
        # raw_df = raw_df.selectExpr( "S_no", "eventId", "eventOffset", "eventPublisher", "customerId", "deviceId", "measure", "status", "temperature", "eventTime")
        return raw_df