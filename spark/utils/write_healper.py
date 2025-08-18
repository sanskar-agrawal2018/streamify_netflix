from pyspark.sql import DataFrame
from typing import Optional
class WriteHelper:
    """
    A helper class for writing files.
    """

    @staticmethod
    def write_stream(df: DataFrame,format: str, path: Optional[str],checkpoint: Optional[str]) -> None:
        print('checkpoint', checkpoint,'format', format)
        if(format =="console"):
            
            if checkpoint:
                df.writeStream.format(format).option("checkpointLocation", checkpoint).start()
            else:
                df.writeStream.format(format).start()
        else:
            df = df.writeStream.format(format).option("path", path)
            if checkpoint:
                print("Inside checkpoint check", checkpoint)
                df = df.option("checkpointLocation", checkpoint)
            df.start()
