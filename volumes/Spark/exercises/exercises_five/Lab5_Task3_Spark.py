from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

broker1 = "course-kafka:9092"
my_Topic = "gps-user-review-source"

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("ex5_reviews_kafka_spark")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate()
    )

schem = T.StructType([
    T.StructField("application_name", T.StringType(), True),
    T.StructField("translated_review", T.StringType(), True),
    T.StructField("sentiment_rank", T.LongType(), True),
    T.StructField("sentiment_polarity", T.FloatType(), True),
    T.StructField("sentiment_subjectivity", T.FloatType(), True)
])

reviews_df = spark.readStream.schema(schem).parquet("s3a://googleplaystore/source/google_reviews")
#reviews_df = spark.read.parquet("s3a://googleplaystore/source/google_reviews")
#reviews_df.printSchema()

queryProducer = (
    reviews_df
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker1)
    .option("topic", my_Topic)
    .option("checkpointLocation", "s3a://googleplaystore/kafka_checkpoints/Producers_Checkpoints/google_reviews")
    .start()
    .awaitTermination()
)


# queryComsumer = (
#     queryProducer
#     .readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", broker1)
#     .option("subscribe", my_Topic)
#     .option("strtingOffsets", "earliest")
#     .option("checkpointLocation", "s3a://googleplaystore/kafka_checkpoints/Consumers_Checkpoints/google_reviews")
#     .load()
#     .selectExpr("CAST(value AS STRING) AS json_value")
# )



spark.stop()