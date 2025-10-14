from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import errors as kafka_errors

####Define variables
spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("DataEnrichment")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate()
)

course_broker = "course-kafka:9092"
input_topic = "sensors-sample"
output_topic = "samples-enriched"

schem = T.StructType([
    T.StructField("event_id", T.IntegerType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("car_id", T.StringType()),
    T.StructField("speed", T.IntegerType()),
    T.StructField("rpm", T.IntegerType()),
    T.StructField("gear", T.IntegerType())
])
###############

models_df = spark.read.csv("s3a://sparkproject/data/dims/car_models.csv", header=True)
colors_df = spark.read.csv("s3a://sparkproject/data/dims/car_colors.csv", header=True)
cars_df = spark.read.csv("s3a://sparkproject/data/dims/cars.csv", header=True)

models_df.cache()
colors_df.cache()
colors_df.cache()


raw_data_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", course_broker)
    .option("subscribe", input_topic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false") #Don't fall it some data is lost
    .load()
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(F.from_json(F.col("json_value"), schem).alias("value"))
    .select("value.*")
)
#raw_data_df.writeStream.format("console").outputMode("append").start().awaitTermination() #print batch

enriched_df = (
    raw_data_df
    .join(cars_df, on="car_id", how="inner")
    .join(models_df, on="model_id", how="inner")
    .join(colors_df, on="color_id", how="inner")
    .select(
        F.col("event_id"),
        F.col("event_time"),
        F.col("car_id"),
        F.col("speed"),
        F.col("rpm"),
        F.col("gear"),
        F.col("driver_id"),
        F.col("car_brand").alias("brand_name"),
        F.col("car_model").alias("model_name"),
        F.col("color_name"),
        F.round(F.col("speed")/30).cast(T.IntegerType()).alias("expected_gear")
    )
)
#enriched_df.writeStream.format("console").outputMode("append").start().awaitTermination() #print batch

#create kafka topic
admin_client = KafkaAdminClient(bootstrap_servers=[course_broker])
new_topic = NewTopic(name=output_topic, num_partitions=1, replication_factor=1)

try:
    admin_client.create_topics(new_topics=[new_topic], validate_only=False)
except kafka_errors.TopicAlreadyExistsError:
    print("="*100)
    print(f"\n\nDidn't create a new topic, topic \"{output_topic}\" already exists\n\n")
    print("="*100, "\n")
except kafka_errors.InvalidReplicationFactorError as rf:
    print("="*100, f"\n\nError {rf.errno}: {rf.message}\n\n", "="*100)


### Push enriched data into the "samples-enriched" topic
enriched_producer = (
    enriched_df
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", course_broker)
    .option("topic", output_topic)
    .option("checkpointLocation", "/tmp/spark_project_checkpoints/samples-enriched")
    .outputMode("append")
    .start()
    .awaitTermination()
)

models_df.unpersist()
colors_df.unpersist()
colors_df.unpersist()

spark.stop()