from pyspark.sql import SparkSession, Row

def create_dims_models_and_colors():
    spark = (
        SparkSession
        .builder
        .master("local[*]")
        .appName("ModelCreation")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
        .getOrCreate()
        )
    
    ##Car Models
    car_models_list = [
        Row(model_id=1, car_brand="Mazda", car_model="3"),
        Row(model_id=2, car_brand="Mazda", car_model="6"),
        Row(model_id=3, car_brand="Toyota", car_model="Corolla"),
        Row(model_id=4, car_brand="Hyundai", car_model="i20"),
        Row(model_id=5, car_brand="Kia", car_model="Sportage"),
        Row(model_id=6, car_brand="Kia", car_model="Rio"),
        Row(model_id=7, car_brand="Kia", car_model="Picanto")
    ]
    
    car_models_df = spark.createDataFrame(car_models_list)
    # car_models_df.printSchema()
    # car_models_df.show()
    
    car_models_df.write.mode("overwrite").csv("s3a://sparkproject/data/dims/car_models.csv", header=True)
    
    ##Car Colors
    car_colors_list = [
        Row(color_id=1, color_name="Black"),
        Row(color_id=2, color_name="Red"),
        Row(color_id=3, color_name="Gray"),
        Row(color_id=4, color_name="White"),
        Row(color_id=5, color_name="Green"),
        Row(color_id=6, color_name="Blue"),
        Row(color_id=7, color_name="Pink")
    ]
    
    car_colors_df = spark.createDataFrame(car_colors_list)
    # car_colors_df.printSchema()
    # car_colors_df.show()
    
    car_colors_df.write.mode("overwrite").csv("s3a://sparkproject/data/dims/car_colors.csv", header=True)
    
    spark.stop()