
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json 
from pyspark.sql.types import StructType, StringType, LongType, ArrayType, StructField,TimestampType
from utils.write_healper import WriteHelper
from pyspark.sql.functions import lit
class ReadHelper:
    """
    A helper class for reading files.
    """

    @staticmethod
    def read_stream(spark :SparkSession,path:str,schema)-> DataFrame:
        df = None
        if(schema is None):
            df=spark.readStream.json(path)
        else:
            df=spark.readStream.schema(schema).json(path)

    
        schema=StructType([
            StructField("customerId", StringType(), True),
            StructField("data", StructType([
                StructField("devices", ArrayType(StructType([
                    StructField("deviceId", StringType(), True),
                    StructField("measure", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("temperature", LongType(), True)
                ])), True)
            ]), True),
            StructField("eventId", StringType(), True),
            StructField("eventOffset", LongType(), True),
            StructField("eventPublisher", StringType(), True),
            StructField("eventTime", TimestampType(), True)
        ])
        df=df.selectExpr("CAST(value AS STRING) as value")  # Ensure 'value' is a string

        #Dead letter queue handling
        
        df=df.select(from_json("value", schema).alias("data")).select("data.*")  # Parse JSON string into columns
        df_null = df.filter(df["eventId"].isNull() | df["eventOffset"].isNull() | df["eventPublisher"].isNull() | df["customerId"].isNull() | df["data"].isNull())
        df_null =df_null.withColumn("error_message", lit("Null value found"))
    
        # print("Null values found in the following columns:")
        WriteHelper.write_stream(df_null, "console", None, None)  # Write null values to console for debugging
        df_not_null = df.filter(~(df["eventId"].isNull() | df["eventOffset"].isNull() | df["eventPublisher"].isNull() | df["customerId"].isNull() | df["data"].isNull()))
        return df_not_null
    


    def read_kafka(spark: SparkSession,server: str, topic: str) -> DataFrame:
        """
        Read data from a Kafka topic.
        
        Args:
            server (str): The Kafka server address.
        
        Returns:
            DataFrame: The DataFrame containing the data read from Kafka.
        """
        df= spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", server)\
            .option("subscribe", topic)\
            .option("startingOffsets", "earliest")\
            .load()
        # df.show()
        return df