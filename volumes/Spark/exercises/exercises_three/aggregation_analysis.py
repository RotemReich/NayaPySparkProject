from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("ex3_aggregation_analysis")
    .getOrCreate()
)

flights_df = spark.read.parquet("s3a://spark/transformed/flights")
airports_df = spark.read.parquet("s3a://spark/parques/airports/")

# print(">>>flights schema is:")
# flights_df.printSchema()
# print(">>>airports schema is:")
# airports_df.printSchema()

print(">>>>> Top 10 Airports by Departures:")
(
    flights_df
    .groupBy(F.col("origin_airport_id").alias("airport_id"))
    .agg(F.count(F.lit(1)).alias("num_departures"))
    .join(airports_df.select(F.col("airport_id"), F.col("name").alias("airport_name")), ["airport_id"])
    .orderBy(F.col("num_departures").desc())
    .show(10)
)

print(">>>>> Top 10 Airports by Arrivals:")
(
    flights_df
    .groupBy(F.col("dest_airport_id").alias("airport_id"))
    .agg(F.count(F.lit(1)).alias("num_arrivals"))
    .join(airports_df.select(F.col("airport_id"), F.col("name").alias("airport_name")), ["airport_id"])
    .orderBy(F.col("num_arrivals").desc())
    .show(10)
)

print(">>>>> Top 10 Flight Routes:")
(
    flights_df
    .groupBy(F.col("origin_airport_id"), F.col("dest_airport_id"))
    .agg(F.count(F.lit(1)).alias("num_of_routes"))
    .join(airports_df.select(F.col("airport_id").alias("origin_airport_id"), F.col("name").alias("origin_airport_name")), ["origin_airport_id"])
    .join(airports_df.select(F.col("airport_id").alias("dest_airport_id"), F.col("name").alias("dest_airport_name")), ["dest_airport_id"])
    .select(F.concat_ws(" -> ", F.col("origin_airport_name"), F.col("dest_airport_name")).alias("Route"), F.col("num_of_routes"))
    .orderBy(F.col("num_of_routes").desc())
    .show(10)
)


spark.stop()