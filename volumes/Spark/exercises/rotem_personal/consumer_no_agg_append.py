#append
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession
    .builder
    .appName("rotem_try")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate()
)

broker = "course-kafka:9092"
my_Topic = "Rotem-Topic"

schem = T.StructType([
    T.StructField("name", T.StringType()),
    T.StructField("number", T.IntegerType())
])


queryComsumer = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("subscribe", my_Topic)
    .option("startingOffsets", "earliest")
    #.option("checkpointLocation", "/tmp/spark_checkpoints/no-agg-append")
    .load()
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(F.from_json(F.col("json_value"), schem).alias("value"))
    .select("value.*")
)
#queryComsumer.writeStream.format("console").outputMode("append").start().awaitTermination() #print batch

TopicTo = "no-agg-append"

queryProducer = (
    queryComsumer
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", broker)
    .option("topic", TopicTo)
    .option("checkpointLocation", "/tmp/spark_checkpoints/no-agg-append")
    .outputMode("append")
    .start()
    .awaitTermination()
)

spark.stop()