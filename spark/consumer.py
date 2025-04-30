from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("UberRidesStream") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # or "WARN"

schema = StructType() \
    .add("Request id", StringType()) \
    .add("Pickup point", StringType()) \
    .add("Driver id", StringType()) \
    .add("Status", StringType()) \
    .add("Request timestamp", StringType()) \
    .add("Drop timestamp", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rides-topic") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

aggregated = parsed_df.groupBy("Pickup point").count()

query = aggregated.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
