from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import date

spark = SparkSession.builder.appName("DailyReport").getOrCreate()

# Read the parquet data
df = spark.read.parquet("output/stream_data")

df.printSchema()
df.show(5, truncate=False)

# Rename the 'sum(total_amount)' column to 'total_amount'
df = df.withColumnRenamed("sum(total_amount)", "total_amount")

df.printSchema()
df.show(5, truncate=False)

df.createOrReplaceTempView("rides")

result = spark.sql("""
    SELECT
      pickup_latitude,
      pickup_longitude,
      COUNT(*) AS total_rides,
      SUM(total_amount) AS total_earnings
    FROM rides
    GROUP BY pickup_latitude, pickup_longitude
""")

today = date.today().isoformat()
# Use CSV format instead of text, and include a header
result.coalesce(1).write.mode("overwrite").csv(f"output/daily_report_{today}.csv", header=True)

spark.stop()
