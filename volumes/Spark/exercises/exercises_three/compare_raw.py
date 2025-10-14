from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .appName("ex3_compare_raw")
    .getOrCreate()
)

flights_df = spark.read.parquet("s3a://spark/parques/flight")
flights_raw_df = spark.read.parquet("s3a://spark/parques/flights_raw")

flights_df_distinct = flights_df.dropDuplicates()
flights_raw_df_distict = flights_raw_df.dropDuplicates()

matched = flights_df_distinct.intersect(flights_raw_df_distict)

unmatched_flights = flights_df_distinct.subtract(matched).withColumn("Source", F.lit("flights"))
unmatched_flights_raw = flights_raw_df_distict.subtract(matched).withColumn("Source", F.lit("flights_raw"))

unmatched = unmatched_flights.union(unmatched_flights_raw)

# matched.show(5)
# print("============================================================================================================\n"*3)
# unmatched_flights.show(5)
# print("============================================================================================================\n"*3)
# unmatched_flights_raw.show(5)

matched.write.parquet("s3a://spark/stg/flight_matched", mode="overwrite")
unmatched.write.parquet("s3a://spark/stg/fligth_unmatched", mode="overwrite")

spark.stop()