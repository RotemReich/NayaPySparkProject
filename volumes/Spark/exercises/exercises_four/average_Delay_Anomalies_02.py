from pyspark.sql import SparkSession
#from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("ex4_anomalies_detection")
    .getOrCreate()
)

unbounded_window = Window.partitionBy(F.lit(1))

flights_df = spark.read.parquet("s3a://spark/transformed/flights")

flights_df.cache()

# print(">>>flights schema is:")
# flights_df.printSchema()

(
    flights_df
    .withColumn("avg_all_time",
              F.round(
                        (F.sum(F.col("arr_delay")).over(unbounded_window) / F.count(F.lit(1)).over(unbounded_window))
                    ,4)
        )
    .withColumn("avg_diff_percent",
              F.abs(
                  F.round(
                            ((F.sum(F.col("arr_delay")).over(unbounded_window) / F.count(F.lit(1)).over(unbounded_window))
                             -
                             F.col("arr_delay")
                            )
                            /
                            (F.sum(F.col("arr_delay")).over(unbounded_window) / F.count(F.lit(1)).over(unbounded_window))
                        ,4)
                    )
        )
    .where(F.col("avg_diff_percent") > 5)
    .show()
)

flights_df.unpersist()

spark.stop()