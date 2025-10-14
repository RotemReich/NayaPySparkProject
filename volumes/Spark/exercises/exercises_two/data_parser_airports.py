from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("ex2_airports").getOrCreate()

airports_raw_df = spark.read.csv("s3a://spark/airports.csv", header=True)

#airports_raw_df.show()
airports_df = (
    airports_raw_df.select(
        F.col("airport_id").cast(T.IntegerType()).alias("airport_id"),
        F.col("city"),
        F.col("state"),
        F.col("name")
    )
)

#airports_df.cashe()
#airports_df.show()

airports_df.write.parquet("s3a://spark/parques/airports/", mode="overwrite")

spark.stop()