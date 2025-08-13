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
        return raw_df.withColumn("name", F.upper(F.col("name")))