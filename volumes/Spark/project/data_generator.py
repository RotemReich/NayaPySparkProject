from kafka import KafkaProducer
from kafka import errors as kafka_errors
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import os
import time

JARs = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2","org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901"]
#define spark session
spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("DataGenerator")
    .config("spark.jars.packages", ",".join(JARs))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # MinIO service name from docker-compose
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

rn = 0
course_broker = "course-kafka:9092"
my_Topic = "sensors-sample"

#create kafka producer
producer = KafkaProducer(
    bootstrap_servers=course_broker,
    value_serializer=lambda msg: msg.encode("UTF-8")
)

#create kafka topic
admin_client = KafkaAdminClient(bootstrap_servers=[course_broker])
new_topic = NewTopic(name=my_Topic, num_partitions=1, replication_factor=1)

try:
    admin_client.create_topics(new_topics=[new_topic], validate_only=False)
except kafka_errors.TopicAlreadyExistsError:
    print("="*100, f"\n\nDidn't create a new topic, topic \"{my_Topic}\" already exists\n\nReading last event_id\n")
    schem = T.StructType([
        T.StructField("event_id", T.IntegerType()),
        T.StructField("event_time", T.TimestampType()),
        T.StructField("car_id", T.StringType()),
        T.StructField("speed", T.IntegerType()),
        T.StructField("rpm", T.IntegerType()),
        T.StructField("gear", T.IntegerType())
    ])    
    raw_data_df = (
        spark
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", course_broker)
        .option("subscribe", my_Topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(F.from_json(F.col("json_value"), schem).alias("value"))
        .select("value.*")
        .select(F.col("event_id"))
    )
    #raw_data_df.show()    
    rn = raw_data_df.agg(F.max(F.col("event_id")).alias("event_id")).first()["event_id"]
    
    print(f"next 'event_id' will be {rn+1}\n\n")
except kafka_errors.InvalidReplicationFactorError as rf:
    print("="*100, f"\n\nError {rf.errno}: {rf.message}\n\n", "="*100)


cars_df = spark.read.csv("s3a://sparkproject/data/dims/cars.csv", header=True)
cars_df.cache()
# cars_df.show()

while True:    
    data_gen_df = (
        cars_df
        .withColumn("event_id", F.row_number().over(Window.orderBy("car_id")) + rn)
        .withColumn("event_time", F.current_timestamp())
        .withColumn("speed", F.floor(F.rand()*201))
        .withColumn("rpm", F.floor(F.rand()*8001))
        .withColumn("gear", F.ceil(F.rand()*7))
        .select(
            F.col("event_id").cast(T.IntegerType()).alias("event_id"),
            F.col("event_time").cast(T.TimestampType()).alias("event_time"),
            F.col("car_id").cast(T.StringType()).alias("car_id"),
            F.col("speed").cast(T.IntegerType()).alias("speed"),
            F.col("rpm").cast(T.IntegerType()).alias("rpm"),
            F.col("gear").cast(T.IntegerType()).alias("gear")
        )
        #.show(20)
    )
    
    for json_str in data_gen_df.toJSON().collect():
        producer.send(topic=my_Topic, value=json_str)
    
    producer.flush()
    rn += 20
    time.sleep(1)

cars_df.unpersist()
spark.stop()