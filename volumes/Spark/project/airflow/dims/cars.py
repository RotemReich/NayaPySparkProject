from pyspark.sql import SparkSession, Row
import random

def create_dim_cars():
    spark = (
        SparkSession
        .builder
        .master("local[*]")
        .appName("CarsGenerator")
        #.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
        .getOrCreate()
        )

    cars_list = [
    Row(
        car_id = ''.join([str(random.randint(0,9)) for i in range(7)]), #so that car_id could start with 0
        driver_id = str(random.randint(200_000_000,399_999_999)), #person ID only can start with 2 or 3
        model_id = random.randint(1,7),
        color_id = random.randint(1,7)
    )
    for i in range(20)
]

    cars_df = spark.createDataFrame(cars_list)
    # cars_df.show(20)

    cars_df.write.mode("overwrite").csv("s3a://sparkproject/data/dims/cars.csv", header=True)

    spark.stop()