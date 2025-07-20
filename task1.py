# TASK 1: BIG DATA ANALYSIS USING PYSPARK

# --- Setup ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, hour, dayofweek, count

# Create Spark Session
spark = SparkSession.builder \
    .appName("NYC Taxi Big Data Analysis") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2023-01.parquet")

# --- Basic Overview ---
df.printSchema()
df.show(5)

# --- Data Cleaning ---
# Filter out invalid trips
df_clean = df.filter((col("trip_distance") > 0) & (col("fare_amount") > 0))

# --- Analysis 1: Average Trip Distance by Day of Week ---
df_day = df_clean.withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
avg_dist_per_day = df_day.groupBy("day_of_week").agg(avg("trip_distance").alias("avg_trip_distance"))
avg_dist_per_day.orderBy("day_of_week").show()

# --- Analysis 2: Peak Hours (Trip Count by Hour) ---
df_hour = df_clean.withColumn("hour", hour("tpep_pickup_datetime"))
trips_per_hour = df_hour.groupBy("hour").agg(count("*" ).alias("trip_count"))
trips_per_hour.orderBy("hour").show()

# --- Analysis 3: Average Fare per Mile ---
df_clean = df_clean.withColumn("fare_per_mile", col("fare_amount") / col("trip_distance"))
avg_fare_per_mile = df_clean.agg(avg("fare_per_mile")).collect()[0][0]
print(f"\nAverage Fare per Mile: ${avg_fare_per_mile:.2f}")

# --- Save Summary to CSV (Optional) ---
avg_dist_per_day.toPandas().to_csv("avg_trip_distance_by_day.csv")
trips_per_hour.toPandas().to_csv("trip_count_by_hour.csv")

# --- Stop Spark ---
spark.stop()

# Save average trip distance by day of week
#avg_distance_by_day.coalesce(1).write.option("header", True).csv("output/avg_distance_by_day")

# Save trip count by hour
#trip_count_by_hour.coalesce(1).write.option("header", True).csv("output/trip_count_by_hour")