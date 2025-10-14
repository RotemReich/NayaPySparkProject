from pyspark.sql import SparkSession
#from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .appName("ex4_anomalies_detection")
    .getOrCreate()
)

sliding_range_window = Window.partitionBy(F.col("carrier")).orderBy(F.col("start_range"))

flights_df = spark.read.parquet("s3a://spark/transformed/flights")

#flights_df.cache()

# print(">>>flights schema is:")
# flights_df.printSchema()

(
    flights_df
    .groupBy(F.col("carrier"), F.window(F.col("flight_date"), "10 days", "1 day").alias("date_window"))
    .agg(F.sum(F.col("dep_delay") + F.col("arr_delay")).alias("total_delay"))
    .select(F.col("carrier"), F.col("date_window.start").alias("start_range"), F.col("date_window.end").alias("end_range"), F.col("total_delay"))
    .withColumn("last_window_delay", F.lag(F.col("total_delay")).over(sliding_range_window))
    .withColumn("change_percent", F.abs((1 - F.col("total_delay")) / F.col("last_window_delay")))
    .where(F.col("change_percent") > 0.3)
    .show(5)
)

#flights_df.unpersist()

spark.stop()