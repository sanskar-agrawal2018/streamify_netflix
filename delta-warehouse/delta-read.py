from delta import DeltaTable
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def create_spark_session(app_name: str):
    """
    Create a Spark session with the necessary configurations.
    """

    builder = SparkSession.builder.appName(app_name)\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


if __name__ == "__main__":
    spark = create_spark_session("Delta Read Example")

    # Initialize DeltaTable object
    delta_table = DeltaTable.forPath(spark, "/home/sanskar/Project/streamify/spark/data/data_device_data")

    # Read the Delta table
    df = delta_table.toDF()

    # Show the DataFrame
    df.show()
