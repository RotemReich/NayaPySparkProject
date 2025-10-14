from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("ex2_flights_raw").getOrCreate()

schem = T.StructType([
    T.StructField("DayofMonth", T.IntegerType(), True),
    T.StructField("DayOfWeek", T.IntegerType(), True),
    T.StructField("Carrier", T.StringType(), True),
    T.StructField("OriginAirportID", T.IntegerType(), True),
    T.StructField("DestAirportID", T.IntegerType(), True),
    T.StructField("DepDelay", T.IntegerType(), True),
    T.StructField("ArrDelay", T.IntegerType(), True),
])

flights_raw_df = spark.read.csv("s3a://spark/raw-flight-data.csv", header=True, schema=schem)

df_raw_flights = (
    flights_raw_df.select(
        F.col("DayofMonth").cast(T.IntegerType()).alias("day_of_month"),
        F.col("DayOfWeek").cast(T.IntegerType()).alias("day_of_week"),
        F.col("Carrier").alias("carrier"),
        F.col("OriginAirportID").cast(T.IntegerType()).alias("origin_airport_id"),
        F.col("DestAirportID").cast(T.IntegerType()).alias("dest_airport_id"),
        F.col("DepDelay").cast(T.IntegerType()).alias("dep_delay"),
        F.col("ArrDelay").cast(T.IntegerType()).alias("arr_delay"),
    )
)

#df_raw_flights.show()

df_raw_flights.write.parquet("s3a://spark/parques/flights_raw", mode="overwrite")

spark.stop()