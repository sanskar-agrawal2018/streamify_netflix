import os 
import sys 
import pyspark 
from delta import configure_spark_with_delta_pip
# import delta
from dotenv import load_dotenv
from transformations.data_transforms import DataTransforms
from utils.write_healper import WriteHelper
from utils.read_healper import ReadHelper

load_dotenv(override=True)  # Load environment variables from .env file


def create_spark_session(app_name: str):
    """
    Create a Spark session with the necessary configurations.
    """
    

    
    # os.environ.pop("SPARK_HOME", None)  # Ensure SPARK_HOME is not set

        # .config("spark.jars", "/home/sanskar/Downloads/spark-streaming-kafka-0-10_2.12-3.5.6.jar") \
    # All required JARs
    # jars_dir = "/home/sanskar/Project/streamify/spark/jars"
    # jars = [
    #     f"{jars_dir}/spark-sql-kafka-0-10_2.12-3.5.6.jar",
    #     f"{jars_dir}/kafka-clients-3.5.2.jar",
    #     f"{jars_dir}/lz4-java-1.8.0.jar",
    #     f"{jars_dir}/snappy-java-1.1.8.4.jar",
    #     f"{jars_dir}/commons-pool2-2.11.1.jar",
    #     f"{jars_dir}/slf4j-api-1.7.36.jar"
    # ]
    
    # spark = pyspark.sql.SparkSession.builder.appName(app_name) \
    #     .config("spark.jars", ",".join(jars)) \
    #     .getOrCreate()

    # spark = pyspark.sql.SparkSession.builder.appName(app_name) \
    #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    #     .getOrCreate()
    


    # spark = pyspark.sql.SparkSession.builder.appName(app_name) \
    #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    #     .getOrCreate()

    builder = pyspark.sql.SparkSession.builder.appName(app_name)\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder,["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"]).getOrCreate()
    return spark

def main():
    print(pyspark.__version__)
    print(sys.executable)

    app_name = "MyApp"
    spark = create_spark_session(app_name)
    
    df=spark.read.json(os.path.join(os.getcwd(), "tests","data","device_data"))
    print(df.printSchema())
    schema=df.schema

    

    path=os.path.join(os.getcwd(), "tests","data","device_data")
    print(f"Reading from path: {path}")
    df_stream=ReadHelper.read_stream(spark, path, schema)


    transformed_df = DataTransforms.transform_data(spark, df_stream)
    transformed_df.printSchema()


    checkpoint_path = os.path.join(os.getcwd(), "checkpoint_dir")
    checkpoint_path = os.path.join(checkpoint_path, f"checkpoint_{app_name}")
    WriteHelper.write_stream(transformed_df, "console", None, checkpoint_path)
    if(os.getenv("ENV") == "local"):
        input("Press Enter to exit...")  # Wait for user input before exiting
        spark.stop()
    else:
        # If running in a non-interactive environment, ensure Spark session is stopped
        spark.stop()
        print("Spark session stopped.")

def kafka_transform():
    """
    Example function to read from Kafka and transform data.
    """
    spark = create_spark_session("KafkaTransformApp")
    server = "localhost:9092"
    topic = "device_data"

    print("spark Node :-",spark)
    
    df_kafka = ReadHelper.read_kafka(spark, server, topic)
    # transformed_df = DataTransforms.transform_data(spark, df_kafka)
    
    # df_kafka.printSchema()
    WriteHelper.write_stream(df_kafka, "delta", f'data/data_{topic}', f'checkpoint_dir/checkpoint_{topic}')
    # df_kafka = spark.createDataFrame(
    #     [
    #         {"device_id": "123", "timestamp": "2023-10-01T12:00:00Z", "value": 42},
    #         {"device_id": "456", "timestamp": "2023-10-01T12:05:00Z", "value": 36}
    #     ]
    # )

    # df_kafka.show()
    if(os.getenv("ENV") == "local"):
        input("Press Enter to exit...")  # Wait for user input before exiting
        spark.stop()
    else:
        # If running in a non-interactive environment, ensure Spark session is stopped
        spark.stop()
        print("Spark session stopped.")



if __name__ == "__main__":
    # main()
    print(pyspark.__version__)
    print(os.environ.get("JAVA_HOME"))
    # print(os.environ.get("PATH"))
    # print(os.environ.get("SPARK_HOME"))
    kafka_transform()