import os 
import sys 
import pyspark 
from delta import configure_spark_with_delta_pip
# import delta
from dotenv import load_dotenv

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


def transformation(spark):
    """
    Perform a sample transformation using Delta Lake.
    """
    
    # Create a sample DataFrame
    data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
    df = spark.createDataFrame(data, ["name", "id"])
    
    # Write the DataFrame to a Delta table
    df.write.format("delta").mode("overwrite").save("/tmp/delta-table")
    
    # Read from the Delta table
    df2 = spark.read.format("delta").load("/tmp/delta-table")
    
    # Show the contents of the Delta table
    df2.show()

if __name__ == "__main__":
    print(pyspark.__version__)
    print(sys.executable)
    spark = create_spark_session()
    

    transformation(spark)
    
    if(os.getenv("ENV") == "local"):
        input("Press Enter to exit...")  # Wait for user input before exiting
        spark.stop()
    else:
        # If running in a non-interactive environment, ensure Spark session is stopped
        spark.stop()
        print("Spark session stopped.")