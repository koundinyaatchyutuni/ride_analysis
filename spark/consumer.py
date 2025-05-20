#import statements
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#creating spark 
spark = SparkSession.builder \
    .appName("UberRidesStream") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR") 
#stucture declaration of data types and columns
schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", StringType()),
    StructField("tpep_dropoff_datetime", StringType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("pickup_longitude", DoubleType()),
    StructField("pickup_latitude", DoubleType()),
    StructField("RatecodeID", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("dropoff_longitude", DoubleType()),
    StructField("dropoff_latitude", DoubleType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType())
])

#to read the stream of data form kafka
# In your consumer.py (or wherever you read from Kafka)
df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "your_kafka_servers") \
  .option("subscribe", "your_topic") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false").load()

#this is used to convert the bit stream data to string with a key null value the data
parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("pickup_time", to_timestamp("tpep_pickup_datetime"))

# Sample Analysis: Total amount collected by location every minute
aggregated = parsed_df \
    .withWatermark("pickup_time", "1 minute") \
    .groupBy(window("pickup_time", "1 minute"), "pickup_latitude", "pickup_longitude") \
    .sum("total_amount") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/stream_data") \
    .option("checkpointLocation", "output/checkpoints") \
    .start()

aggregated.awaitTermination()
#data analysis

# aggregated = parsed_df.groupBy("Pickup point").count()

# #writing the data analysis results to console
# query = aggregated.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# query.awaitTermination()