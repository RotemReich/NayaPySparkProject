from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession
    .builder
    .appName("Lab6_Task1")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate()
)

google_apps_df = spark.read.parquet("s3a://googleplaystore/source/google_apps")
google_apps_df.cache()
#google_apps_df.printSchema()

broker = "course-kafka:9092"
TopicFrom = "gps-user-review-source"

schem = T.StructType([
    T.StructField("application_name", T.StringType()),
    T.StructField("translated_review", T.StringType()),
    T.StructField("sentiment_rank", T.IntegerType()),
    T.StructField("sentiment_polarity", T.FloatType()),
    T.StructField("sentiment_subjectivity", T.FloatType())
])


queryComsumer = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", TopicFrom)
    .option("startingOffsets", "earliest")
    .option("checkpointLocation", "s3a://googleplaystore/kafka_checkpoints/Producers_Checkpoints/gps-user-review-source")
    .load()
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(F.from_json(F.col("json_value"), schem)
            .alias("value")
            )
    .select("value.*")
)
#queryComsumer.writeStream.format("console").outputMode("append").start().awaitTermination() #print batch

enriched_df = (
    queryComsumer
    .groupBy(F.col("application_name"))
    .agg(
        F.sum(F.when(F.col("sentiment_rank")==1,1).otherwise(0)).alias("positive_reviews_cnt"),
        F.sum(F.when(F.col("sentiment_rank")==0,1).otherwise(0)).alias("neutral_reviews_cnt"),
        F.sum(F.when(F.col("sentiment_rank")==-1,1).otherwise(0)).alias("negative_reviews_cnt"),
        (F.sum(F.col("sentiment_polarity")) / F.sum(F.lit(1))).alias("avg_sentiment_polarity"),
        (F.sum(F.col("sentiment_subjectivity")) / F.sum(F.lit(1))).alias("avg_sentiment_subjectivity")
    )
    .join(google_apps_df, ["application_name"])
)
#enriched_df.writeStream.format("console").outputMode("complete").start().awaitTermination() #print batch

TopicTo = "gps-with-reviews"

queryProducer = (
    enriched_df
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("topic", TopicTo)
    .option("checkpointLocation", "s3a://googleplaystore/kafka_checkpoints/Producers_Checkpoints/gps-with-reviews")
    .outputMode("update")
    .start()
    .awaitTermination()
)

google_apps_df.unpersist()
spark.stop()