from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType
import re

input_path = "data/top_10_games_reviews.json"   
output_path = "cleaned_reviews.csv"              
def clean_text(text):
    if text is None:
        return ""
    text = re.sub(r"http\S+", "", text)
    text = text.replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text
spark = SparkSession.builder \
    .appName("CleanSteamReviews") \
    .getOrCreate()

df_raw = spark.read.option("multiline", True).json(input_path)
df_exploded = df_raw.select(F.explode(F.map_entries(F.col("*"))).alias("kv")) \
    .select(
        F.col("kv.key").alias("appid"),
        F.col("kv.value").alias("reviews")
    )
# Explode each list of reviews
df_reviews = df_exploded.select(
    F.col("appid"),
    F.explode("reviews").alias("review")
)

# Flatten the 'author' field into top-level columns
df_flat = df_reviews.select(
    "appid",
    *[F.col(f"review.{c}").alias(c) for c in df_reviews.select("review.*").columns if c != "author"],
    *[F.col(f"review.author.{c}").alias(f"author_{c}") for c in df_reviews.select("review.author.*").columns]
)
timestamp_cols = ["timestamp_created", "timestamp_updated", "author_last_played"]
for col in timestamp_cols:
    if col in df_flat.columns:
        df_flat = df_flat.withColumn(col, F.to_timestamp(F.col(col).cast("double")))
clean_text_udf = F.udf(clean_text, StringType())
df_cleaned = df_flat.withColumn("review", clean_text_udf(F.col("review")))

subset_cols = [col for col in ["author_steamid", "review", "appid"] if col in df_cleaned.columns]
df_dedup = df_cleaned.dropDuplicates(subset=subset_cols)

# Save
df_dedup.write.mode("overwrite").option("header", True).csv(output_path)
print(f"Cleaned data saved to {output_path}")
