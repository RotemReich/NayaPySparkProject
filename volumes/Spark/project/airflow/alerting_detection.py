from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import errors as kafka_errors

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("AlertDetection")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate()
)

course_broker = "course-kafka:9092"
input_topic = "samples-enriched"
output_topic = "alert-data"

schem = T.StructType([
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

enriched_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", course_broker)
    .option("subscribe", input_topic)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false") #Don't fail if some data is lost
    .load()
    .selectExpr("CAST(value as STRING) AS json_value")
    .select(F.from_json(F.col("json_value"), schema=schem).alias("value"))
    .select("value.*")
)
#enriched_df.writeStream.format("console").outputMode("append").start().awaitTermination() #print batch

filtered_df = (
    enriched_df
    .where(
        (F.col("speed") > 120) |
        (F.col("expected_gear") != F.col("gear")) |
        (F.col("rpm") > 6000)
    )
)

###Create new topic
kafka_admin_client = KafkaAdminClient(bootstrap_servers=[course_broker])
new_topic = NewTopic(name=output_topic, num_partitions=1, replication_factor=1)

try:
    kafka_admin_client.create_topics(new_topics=[new_topic], validate_only=False)
except kafka_errors.TopicAlreadyExistsError as ta:
    print("\n\n", "="*100)
    print(f"\n\ntopic \"{output_topic}\" already exists")
    print(ta)
    print("="*100, "\n\n")
except kafka_errors.InvalidReplicationFactorError as rf:
    print("\n\n", "="*100)
    print(rf)
    print("="*100, "\n\n")
except Exception as e:
    pass

filtered_producer = (
    filtered_df
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