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


def create_spark_session():
    """
    Create a Spark session with the necessary configurations.
    """
    print(pyspark.__version__)
    

    
    os.environ.pop("SPARK_HOME", None)  # Ensure SPARK_HOME is not set
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    


    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark




if __name__ == "__main__":
    print(pyspark.__version__)
    print(sys.executable)
    spark = create_spark_session()
    
    df=spark.read.json(os.path.join(os.getcwd(), "tests","data","device_data"))
    print(df.printSchema())
    schema=df.schema

    

    path=os.path.join(os.getcwd(), "tests","data","device_data")
    print(f"Reading from path: {path}")
    df_stream=ReadHelper.read_stream(spark, path, schema)


    transformed_df = DataTransforms.transform_data(spark, df_stream)
    transformed_df.printSchema()

    WriteHelper.write_stream(transformed_df, "console", None, None)
    if(os.getenv("ENV") == "local"):
        input("Press Enter to exit...")  # Wait for user input before exiting
        spark.stop()
    else:
        # If running in a non-interactive environment, ensure Spark session is stopped
        spark.stop()
        print("Spark session stopped.")