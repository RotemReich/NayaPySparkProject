from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import json
from kafka import KafkaProducer, KafkaConsumer
import time
from kafka.admin import KafkaAdminClient, NewTopic

broker1 = "course-kafka:9092"
my_Topic = "gps-user-review-source"

producer = KafkaProducer(
    bootstrap_servers=broker1,
    value_serializer=lambda msg: msg.encode("UTF-8")
)

# admin_client = KafkaAdminClient(bootstrap_servers=[broker1])
# new_topic = NewTopic(name="Lab5_Task3_python_Topic_xxx", num_partitions=2,replication_factor=2)
# try:
#     admin_client.create_topics(new_topics=[new_topic], validate_only=False)
# except Exception as e:
#     print(f"\n\n\n>>>>Error creating topic: {e}\n\n\n")

# consumer = KafkaConsumer(
#     my_Topic, #Topic Name
#     bootstrap_servers=broker1,
#     #group_id = "Lab5_Task3_pythonConsumer_group",
#     value_deserializer=lambda msg: json.loads(msg.decode("UTF-8"))
# )

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("ex5_reviews_kafka_python")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
    )

reviews_df = spark.read.parquet("s3a://googleplaystore/source/google_reviews")
#reviews_df.printSchema()

reviews_json = reviews_df.toJSON()
# for row in reviews_json.take(5):
#     parsed = json.loads(row)
#     print(json.dumps(parsed, indent=4))

i = 0
for json_data in reviews_json.collect():
    i += 1
    producer.send(topic=my_Topic, value=json_data)
    if i==50:
        producer.flush()
        time.sleep(5)
        i = 0
        print("50 messages were sent")

producer.close()
#consumer.close()
spark.stop()