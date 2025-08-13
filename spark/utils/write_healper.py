from pyspark.sql import DataFrame
from typing import Optional
class WriteHelper:
    """
    A helper class for writing files.
    """

    @staticmethod
    def write_stream(df: DataFrame,format: str, path: Optional[str],checkpoint: Optional[str]) -> None:
        if(format =="console"):
            df.writeStream.format(format).start()
        else:
            df.writeStream.format(format).option("path", path)
            if checkpoint:
                df.writeStream.option("checkpointLocation", checkpoint)
            df.writeStream.start()
