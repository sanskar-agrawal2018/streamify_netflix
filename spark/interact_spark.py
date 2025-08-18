cmd=['/home/sanskar/Project/streamify/spark/.venv/lib/python3.13/site-packages/pyspark/./bin/spark-submit', '--conf', 'spark.app.name=KafkaTransformApp', '--conf', 'spark.jar.packages=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0', 'pyspark-shell']
cmd=" ".join(cmd)
print(cmd)