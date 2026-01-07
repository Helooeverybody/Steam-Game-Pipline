from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    IntegerType,
)

spark = SparkSession.builder.appName("SteamMetricsCleanerStream").getOrCreate()

namenode_url = "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000"
output_path = f"{namenode_url}/data/steam/history"
checkpoint_path = f"{namenode_url}/checkpoints/steam_history"

schema = StructType(
    [
        StructField("event_timestamp", StringType(), True),
        StructField("app_id", StringType(), True),
        StructField("player_count", DoubleType(), True),
        StructField("peak_players_monthly", LongType(), True),
    ]
)

raw_stream = (
    spark.readStream.format("kafka")
    .option(
        "kafka.bootstrap.servers", "my-kafka-cluster-kafka-bootstrap.kafka.svc:9092"
    )
    .option("subscribe", "steam-player-counts-raw")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 5000)
    .load()
)

df_parsed = raw_stream.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

df_cleaned = df_parsed.select(
    F.to_timestamp(F.col("event_timestamp")).alias("event_timestamp"),
    F.col("app_id").cast(IntegerType()).alias("app_id"),
    F.round(F.col("player_count"), 2).alias("player_count"),
    F.col("peak_players_monthly").cast(LongType()).alias("peak_players_monthly"),
)

df_final = df_cleaned.filter(
    F.col("event_timestamp").isNotNull() & F.col("app_id").isNotNull()
)

df_final = df_final.withWatermark("event_timestamp", "1 day").dropDuplicates(
    ["app_id", "event_timestamp"]
)

query = (
    df_final.writeStream.outputMode("append")
    .format("parquet")
    .option("path", output_path)
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="1 minute")
    .start()
)

query.awaitTermination()
