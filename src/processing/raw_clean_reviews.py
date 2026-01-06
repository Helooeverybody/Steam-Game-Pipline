from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, BooleanType, IntegerType, TimestampType
)

spark = SparkSession.builder \
    .appName("CleanSteamReviews") \
    .config("spark.sql.caseSensitive", "false") \
    .getOrCreate()

input_path = "s3a://spark-scripts/570.jsonl"
output_path = "s3a://spark-scripts/570.parquet"

author_schema = StructType([
    StructField("steamid", StringType(), True),
    StructField("num_games_owned", IntegerType(), True),
    StructField("num_reviews", IntegerType(), True),
    StructField("playtime_forever", LongType(), True),
    StructField("playtime_last_two_weeks", LongType(), True),
    StructField("playtime_at_review", LongType(), True),
    StructField("last_played", LongType(), True)
])

schema = StructType([
    StructField("recommendationid", StringType(), True),
    StructField("author", author_schema, True), 
    StructField("language", StringType(), True),
    StructField("review", StringType(), True),
    StructField("timestamp_created", LongType(), True),
    StructField("timestamp_updated", LongType(), True),
    StructField("voted_up", BooleanType(), True),
    StructField("votes_up", IntegerType(), True),
    StructField("votes_funny", IntegerType(), True),
    StructField("weighted_vote_score", DoubleType(), True),
    StructField("comment_count", IntegerType(), True),
    StructField("steam_purchase", BooleanType(), True),
    StructField("received_for_free", BooleanType(), True),
    StructField("written_during_early_access", BooleanType(), True),
    StructField("primarily_steam_deck", BooleanType(), True)
])
df_raw = spark.read.schema(schema).option("mode", "PERMISSIVE").json(input_path)
def clean_review_text(col):
    c = F.regexp_replace(col, r"http\S+", "")
    c = F.regexp_replace(c, r"[\r\n\t]", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.regexp_replace(c, r"<(br|li)\s*/?>", "; ")
    c = F.regexp_replace(c, r"<[^>]+>", "")
    c = F.regexp_replace(c, r"&nbsp;", " ")
    return F.trim(F.coalesce(c, F.lit("")))

df_cleaned = df_raw.select(
    F.col("recommendationid"),

    clean_review_text(F.col("review")).alias("review"),
    
    F.col("author.steamid").alias("author_steamid"),

    F.coalesce(F.col("author.num_games_owned"), F.lit(0)).alias("author_num_games_owned"),
    F.coalesce(F.col("author.num_reviews"), F.lit(0)).alias("author_num_reviews"),
    F.coalesce(F.col("author.playtime_forever"), F.lit(0)).alias("author_playtime_forever"),
    F.coalesce(F.col("author.playtime_last_two_weeks"), F.lit(0)).alias("author_playtime_last_two_weeks"),
    F.coalesce(F.col("author.playtime_at_review"), F.lit(0)).alias("author_playtime_at_review"),
    
    F.col("timestamp_created").cast(TimestampType()).alias("timestamp_created"),
    F.col("timestamp_updated").cast(TimestampType()).alias("timestamp_updated"),
    F.col("author.last_played").cast(TimestampType()).alias("author_last_played"),

    F.col("voted_up"),
    F.col("steam_purchase"),
    F.col("received_for_free"),
    F.col("written_during_early_access"),
    F.col("primarily_steam_deck"),
    F.coalesce(F.col("votes_up"), F.lit(0)).alias("votes_up"),
    F.coalesce(F.col("votes_funny"), F.lit(0)).alias("votes_funny"),
    F.coalesce(F.col("weighted_vote_score"), F.lit(0.0)).alias("weighted_vote_score"),
    F.coalesce(F.col("comment_count"), F.lit(0)).alias("comment_count"),

    F.col("language")
)

df_dedup = df_cleaned.dropDuplicates(["recommendationid"])
df_dedup.write.mode("overwrite").parquet(output_path)

print(f"Cleaned data saved to {output_path}")