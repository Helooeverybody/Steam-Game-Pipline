from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType
)
spark = SparkSession.builder \
    .appName("SteamMetricsCleaner") \
    .getOrCreate()
schema = StructType([
    StructField("event_timestamp", StringType(), True), 
    StructField("app_id", StringType(), True),          
    StructField("player_count", DoubleType(), True),
    StructField("peak_players_monthly", LongType(), True)
])

input_path = "s3a://spark-scripts/historical_player_counts_monthly.jsonl"
df = spark.read.schema(schema).json(input_path)
df_cleaned = df.select(
    F.to_timestamp(F.col("event_timestamp")).alias("event_timestamp"),
    F.col("app_id").cast(IntegerType()).alias("app_id"),
    F.round(F.col("player_count"), 2).alias("player_count"),
    F.col("peak_players_monthly").cast(LongType()).alias("peak_players_monthly")
)

df_cleaned = df_cleaned.filter(
    F.col("event_timestamp").isNotNull() & 
    F.col("app_id").isNotNull()
)
output_path = "s3a://spark-scripts/historical_player_counts_monthly.parquet"
df_cleaned.write.mode("overwrite").parquet(output_path)
print(f"Successfully processed data and saved to {output_path}")