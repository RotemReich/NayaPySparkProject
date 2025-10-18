from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import window
from pyspark.sql.window import Window as W
from pyspark.sql import types as T

spark = (
    SparkSession
    .builder
    .appName("AlertCounter")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate()
)
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

course_broker = "course-kafka:9092"
topic_input = "alert-data"

schem_enrich = T.StructType([
    T.StructField("event_id", T.IntegerType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("car_id", T.StringType()),
    T.StructField("speed", T.IntegerType()),
    T.StructField("rpm", T.IntegerType()),
    T.StructField("gear", T.IntegerType()),
    T.StructField("driver_id", T.StringType()),
    T.StructField("brand_name", T.StringType()),
    T.StructField("model_name", T.StringType()),
    T.StructField("color_name", T.StringType()),
    T.StructField("expected_gear", T.IntegerType())
])

raw_data_df =  (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", course_broker)
    .option("subscribe", topic_input)
    .option("startingOffsets", "earliest")
    .option("checkpointLocation", "/tmp/spark_project_checkpoints/alert_counter")
    .option("failOnDataLoss", "false") #Don't fail if some data is lost
    .load()
    .selectExpr("CAST(value as STRING) AS json_value")
    .select(F.from_json(F.col("json_value"), schema=schem_enrich).alias("value"))
    .select("value.*")
)
#raw_data_df.writeStream.format("console").outputMode("append").awaitTermination()

window_dur = "15 minutes"

# aggregated_alerts_windowed_df = (
#     raw_data_df
#     .withWatermark("event_time", window_dur)
#     .groupBy(window(timeColumn="event_time", windowDuration=window_dur, slideDuration="1 second").alias("window"))
#     .agg(
#         F.count(F.lit(1)).alias("num_of_rows"),
#         F.sum(F.when(F.col("color_name")=="Black", 1).otherwise(0)).alias("num_of_black"),
#         F.sum(F.when(F.col("color_name")=="White", 1).otherwise(0)).alias("num_of_white"),
#         F.sum(F.when(F.col("color_name")=="Silver",1).otherwise(0)).alias("num_of_silver"),
#         F.max(F.col("speed")).alias("maximum_speed"),
#         F.max(F.col("gear")).alias("maximum_gear"),
#         F.max(F.col("rpm")).alias("maximum_rpm")
#         )
#     .withColumn("current_time_date", F.date_format(F.expr(f"current_timestamp()"), "dd/MM/yyyy HH:mm:ss"))
#     .withColumn("window_end", F.date_format(F.col("window.end"), "dd/MM/yyyy HH:mm:ss"))
#     #.where(F.col("current_time_date") == F.col("window_end"))
#     .select(
#         #F.col("current_time_date"),
#         F.date_format(F.col("window.start"), "dd/MM/yyyy HH:mm:ss").alias("window_start"),
#         F.col("window_end"),
#         F.col("num_of_rows").cast(T.IntegerType()).alias("num_of_rows"),
#         F.col("num_of_black").cast(T.IntegerType()).alias("num_of_black"),
#         F.col("num_of_white").cast(T.IntegerType()).alias("num_of_white"),
#         F.col("num_of_silver").cast(T.IntegerType()).alias("num_of_silver"),
#         F.col("maximum_speed").cast(T.IntegerType()).alias("maximum_speed"),
#         F.col("maximum_gear").cast(T.IntegerType()).alias("maximum_gear"),
#         F.col("maximum_rpm").cast(T.IntegerType()).alias("maximum_rpm")
#     )
# )

aggregated_alerts_windowed_df = (
    raw_data_df
    .where(F.col("event_time") >= F.expr(f"current_timestamp() - interval 16 minutes"))
    .withWatermark("event_time", window_dur)
    .groupBy(window(timeColumn="event_time", windowDuration=window_dur, slideDuration="1 minute").alias("window"))
    .agg(
        F.count(F.lit(1)).alias("num_of_rows"),
        F.sum(F.when(F.col("color_name")=="Black", 1).otherwise(0)).alias("num_of_black"),
        F.sum(F.when(F.col("color_name")=="White", 1).otherwise(0)).alias("num_of_white"),
        F.sum(F.when(F.col("color_name")=="Silver",1).otherwise(0)).alias("num_of_silver"),
        F.max(F.col("speed")).alias("maximum_speed"),
        F.max(F.col("gear")).alias("maximum_gear"),
        F.max(F.col("rpm")).alias("maximum_rpm")
        )
    .withColumn("window_end", F.date_format(F.col("window.end"), "dd/MM/yyyy HH:mm:ss"))
    .withColumn("last_minute", F.date_format(F.date_trunc("minute", F.current_timestamp()), "dd/MM/yyyy HH:mm:ss"))
    .where(F.col("last_minute") == F.col("window_end"))
    .select(
        #F.col("last_minute"),
        F.date_format(F.col("window.start"), "dd/MM/yyyy HH:mm:ss").alias("window_start"),
        F.col("window_end"),
        F.col("num_of_rows").cast(T.IntegerType()).alias("num_of_rows"),
        F.col("num_of_black").cast(T.IntegerType()).alias("num_of_black"),
        F.col("num_of_white").cast(T.IntegerType()).alias("num_of_white"),
        F.col("num_of_silver").cast(T.IntegerType()).alias("num_of_silver"),
        F.col("maximum_speed").cast(T.IntegerType()).alias("maximum_speed"),
        F.col("maximum_gear").cast(T.IntegerType()).alias("maximum_gear"),
        F.col("maximum_rpm").cast(T.IntegerType()).alias("maximum_rpm")
    )
)

(
    aggregated_alerts_windowed_df
    .writeStream
    .format("console")
    .outputMode("complete")
    .trigger(processingTime="1 minute")
    .start()
    .awaitTermination()
)
