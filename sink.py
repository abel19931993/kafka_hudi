from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

spark = SparkSession.builder.getOrCreate()

# Read a stream of data from Kafka
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "my-topic").load()

# Convert the Kafka data to a Spark DataFrame
json_df = kafka_df.select(from_json(kafka_df.value.cast("string"), schema="my_schema").alias("data"))

# Write the Spark DataFrame to a Hudi table
json_df.writeStream.format("hudi").option("hoodie.datasource.write.operation", "upsert").option("hoodie.table.path", "/path/to/hudi/table").start()