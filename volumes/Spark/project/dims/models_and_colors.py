from pyspark.sql import SparkSession, Row
from pyspark.sql import types as T
import os

# print("\n\n=====================================\n")
# print(os.getenv("AWS_ACCESS_KEY_ID"))
# print(os.getenv("AWS_SECRET_ACCESS_KEY"))
# print("\n=====================================\n")

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("ModelCreation")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")  # MinIO service name from docker-compose
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
    )

models_schema = T.StructType([
    T.StructField("model_id", T.IntegerType(), False),
    T.StructField("car_brand", T.StringType(), False),
    T.StructField("car_model", T.StringType(), False)
])

##>>>>Models
car_models_list = [
    Row(model_id=1, car_brand="Mazda", car_model="3"),
    Row(model_id=2, car_brand="Mazda", car_model="6"),
    Row(model_id=3, car_brand="Toyota", car_model="Corolla"),
    Row(model_id=4, car_brand="Hyundai", car_model="i20"),
    Row(model_id=5, car_brand="Kia", car_model="Sportage"),
    Row(model_id=6, car_brand="Kia", car_model="Rio"),
    Row(model_id=7, car_brand="Kia", car_model="Picanto")
]

car_models_df = spark.createDataFrame(car_models_list, schema=models_schema)
# car_models_df.printSchema()
# car_models_df.show()

car_models_df.write.mode("overwrite").csv("s3a://spark/data/dims/car_models.csv", header=True)

##>>>Colors
colors_schema = T.StructType([
    T.StructField("color_id", T.IntegerType(), False),
    T.StructField("color_name", T.StringType(), False)
])

car_colors_list = [
    Row(color_id=1, color_name="Black"),
    Row(color_id=2, color_name="Red"),
    Row(color_id=3, color_name="Gray"),
    Row(color_id=4, color_name="White"),
    Row(color_id=5, color_name="Green"),
    Row(color_id=6, color_name="Blue"),
    Row(color_id=7, color_name="Pink")
]

car_colors_df = spark.createDataFrame(car_colors_list, schema=colors_schema)
# car_colors_df.printSchema()
# car_colors_df.show()

car_colors_df.write.mode("overwrite").csv("s3a://spark/data/dims/car_colors.csv", header=True)

spark.stop()