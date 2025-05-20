from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

sp=SparkSession.builder.appName("testingapp").getOrCreate()
sc=sp.sparkContext()

data=sc.read.csv('/home/koundinya/Desktop/ride_analysis/data/uber_request_data.csv',header=True,inferSchema=True)

sp.sql("select Driver id,count(*) as total_trips group by Driver id having status='Trip Completed' order by total_trips desc")
