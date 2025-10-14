from pyspark.sql import SparkSession, Row
from kafka import KafkaProducer
import time
from kafka.admin import KafkaAdminClient, NewTopic
#import random

broker = "course-kafka:9092"
my_Topic = "Rotem-Topic"

producer = KafkaProducer(
    bootstrap_servers=broker,
    value_serializer=lambda msg: msg.encode("UTF-8")
)

admin_client = KafkaAdminClient(bootstrap_servers=[broker])
new_topic = NewTopic(name=my_Topic, num_partitions=1,replication_factor=1)
try:
    admin_client.create_topics(new_topics=[new_topic], validate_only=False)
except Exception as e:
    print(f"\n\n\n>>>>Error creating topic: {e}\n\n\n")

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("rotem_try")
    #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
    )

for i in range(10):
    n = i+1
    data_list = [
        Row(name="Rotem", number=n),
        Row(name="Zoe", number=n*2),
        Row(name="Yael", number=n*3)
    ]
    data_df = spark.createDataFrame(data_list)    
    data_json = data_df.toJSON().collect()
    for json_str in data_json:
        producer.send(topic=my_Topic, value=json_str)
    
    producer.flush()    
    print(f"the {i} message was sent")
    time.sleep(15)


producer.close()
spark.stop()