from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("ex5_google_reviews")
    .getOrCreate()
    )

google_reviews_df = spark.read.csv("s3a://googleplaystore/raw/googleplaystore_user_reviews.csv", header=True)

#google_reviews_df.show(10)
#google_reviews_df.printSchema()

manipulated_df = (
    google_reviews_df
    .withColumn(
        "sentiment_rank",
        F.when(F.col("Sentiment")=="Positive", 1)
        .when(F.col("Sentiment")=="Negative", -1)
        .when(F.col("Sentiment")=="Neutral", 0)
        .otherwise(-2)
    )
    .where(F.col("sentiment_rank")!=-2)
    .select(
        F.col("App").cast(T.StringType()).alias("application_name"),
        F.col("Translated_review").cast(T.StringType()).alias("translated_review"),
        F.col("sentiment_rank").cast(T.LongType()).alias("sentiment_rank"),
        F.col("Sentiment_polarity").cast(T.FloatType()).alias("sentiment_polarity"),
        F.col("Sentiment_subjectivity").cast(T.FloatType()).alias("sentiment_subjectivity")
    )
)

#manipulated_df.show(10)

manipulated_df.write.parquet("s3a://googleplaystore/source/google_reviews", mode="overwrite")

spark.stop()