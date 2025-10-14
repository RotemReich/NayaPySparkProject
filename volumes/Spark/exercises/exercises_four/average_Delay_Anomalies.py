from pyspark.sql import SparkSession
#from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("ex4_anomalies_derection")
    .getOrCreate()
)

flights_df = spark.read.parquet("s3a://spark/transformed/flights")

# print(">>>flights schema is:")
# flights_df.printSchema()

all_history_window = Window.partitionBy(F.col("carrier")).orderBy(F.col("flight_date").asc())

#flights_df.cache()

(
    flights_df
    .withColumn("avg_till_now", F.round(F.sum(F.col("arr_delay")).over(all_history_window) / F.count(F.lit(1)).over(all_history_window), 4))
    .withColumn("avg_diff_percent",
                F.round(
                    F.abs(
                        ((F.sum(F.col("arr_delay")).over(all_history_window) / F.count(F.lit(1)).over(all_history_window))
                        -
                        F.col("arr_delay"))
                        /
                        (F.sum(F.col("arr_delay")).over(all_history_window) / F.count(F.lit(1)).over(all_history_window)
                        ))
                    ,4))
    .where(F.col("avg_diff_percent") > 3)
    .show()
)

#flights_df.unpersist()

spark.stop()