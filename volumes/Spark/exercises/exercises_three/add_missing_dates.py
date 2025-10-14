from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Row

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .appName("ex3_add_missing_dates")
    .getOrCreate()
)

def get_dates_df():
    dummy_df = spark.createDataFrame([Row(dummy='x')])
    in_dates_df = dummy_df.select(
        F.explode(F.sequence(F.lit("2020-01-01").cast(T.DateType()), F.lit("2020-12-31").cast(T.DateType())))
        .alias("flight_date")
    )
    return in_dates_df

dates_df = get_dates_df()
#dates_df.show()

dates_days_df = (
    dates_df
    .withColumn("day_of_week", F.dayofweek("flight_date"))
    .withColumn("day_of_month", F.dayofmonth("flight_date"))
    )
#dates_days_df.show()

dates_days_max_df = (
    dates_days_df
    .groupBy(F.col("day_of_week"), F.col("day_of_month"))
    .agg(F.max(F.col("flight_date")).alias("flight_date"))
)
#dates_days_max_df.show()

flights_df = spark.read.parquet("s3a://spark/stg/flight_matched")
#flights_df.show()

enriched_flights = flights_df.join(dates_days_max_df, ["day_of_month", "day_of_week"])
#enriched_flights.show()

enriched_flights.write.parquet("s3a://spark/transformed/flights")


spark.stop()