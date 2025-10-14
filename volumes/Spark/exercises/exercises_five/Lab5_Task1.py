from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("ex5_google_apps")
    .getOrCreate()
    )

google_apps_df = spark.read.csv("s3a://googleplaystore/raw/googleplaystore.csv", header=True)

#google_apps_df.printSchema()

selected_df = (
    google_apps_df
    .withColumn(
        "Content Rating",
        F.when(F.col("Content Rating")=="Adults only 18+", 18)
        .when(F.col("Content Rating")=="Mature 17+", 17)
        .when(F.col("Content Rating")=="Teen", 12)
        .when(F.col("Content Rating")=="Everyone 10+", 10)
        .when(F.col("Content Rating")=="Everyone", 0)
    )
    .where(F.col("Content Rating").isNotNull())
    .select(
        F.col("App").alias("application_name"),
        F.col("Category").alias("category"),
        F.col("Rating").alias("rating"),
        F.col("Reviews").cast(T.FloatType()).alias("reviews"),
        F.col("Size").alias("size"),
        F.regexp_replace(F.col("Installs"), "[^0-9]", "").cast(T.DoubleType()).alias("num_of_installs"),
        F.col("Price").cast(T.DoubleType()).alias("price"),
        F.col("Content Rating").cast(T.DoubleType()).alias("age_limit"),
        F.col("Genres").alias("genres"),
        F.col("Current Ver").alias("version")
        )
    .fillna({"Rating": "-1"})
)


#selected_df.show(6)
#selected_df.printSchema()

selected_df.write.parquet("s3a://googleplaystore/source/google_apps", mode="overwrite")

spark.stop()