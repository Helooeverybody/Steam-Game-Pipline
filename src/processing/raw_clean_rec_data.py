from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
import re

input_path = "s3a://spark-scripts/570.jsonl"
output_path = "s3a://spark-scripts/570.parquet"              
def clean_text(text):
    if text is None:
        return ""
    text = re.sub(r"http\S+", "", text)
    text = text.replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

spark = SparkSession.builder.appName("CleanSteamReviews").getOrCreate()
clean_text_udf = F.udf(clean_text, StringType())

df_raw = spark.read.option("multiline", True).json(input_path)

author_cols = []
if "author" in df_raw.columns:
    author_struct_cols = df_raw.select("author.*").columns
    author_cols = [F.col(f"author.{c}").alias(f"author_{c}") for c in author_struct_cols]

df_flat = df_raw.select(
    F.coalesce(F.col("recommendationid"), F.lit("unknown")).alias("recommendationid"),
    F.coalesce(F.col("review"), F.lit("")).alias("review"),
    *author_cols
)

timestamp_cols = ["timestamp_created", "timestamp_updated", "author_last_played", "author_last_played"]
for col in timestamp_cols:
    if col in df_flat.columns:
        df_flat = df_flat.withColumn(col, F.to_timestamp(F.col(col).cast(DoubleType())))

df_cleaned = df_flat.withColumn("review", clean_text_udf(F.col("review")))

subset_cols = [col for col in ["author_steamid", "review", "recommendationid"] if col in df_cleaned.columns]
df_dedup = df_cleaned.dropDuplicates(subset=subset_cols)


df_dedup.write.mode("overwrite").parquet(output_path)
print(f"Cleaned data saved to {output_path}")
