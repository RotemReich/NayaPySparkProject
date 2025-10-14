from pyspark.sql import SparkSession, Row
from pyspark.sql import types as T
import os
import random

# print("\n\n=====================================\n")
# print(os.getenv("AWS_ACCESS_KEY_ID"))
# print(os.getenv("AWS_SECRET_ACCESS_KEY"))
# print("\n=====================================\n")

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("CarsGenerator")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # MinIO service name from docker-compose
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
    )

cars_schema = T.StructType([
    T.StructField("car_id", T.StringType(), False),
    T.StructField("driver_id", T.StringType(), False),
    T.StructField("model_id", T.IntegerType(), False),
    T.StructField("color_id", T.IntegerType(), False)
])

cars_list = [
    Row(
        car_id = ''.join([str(random.randint(0,9)) for i in range(7)]), #car_id that can start with 0
        driver_id = str(random.randint(200_000_000,399_999_999)), #person ID only can start with 2 or 3
        model_id = random.randint(1,7),
        color_id = random.randint(1,7)
    )
    for i in range(20)
]

cars_df = spark.createDataFrame(cars_list, schema=cars_schema)
# cars_df.show(20)

cars_df.write.mode("overwrite").csv("s3a://spark/data/dims/cars.csv", header=True)

spark.stop()