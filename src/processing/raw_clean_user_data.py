from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

input_path = "data/users_owned_games.json"     
output_path = "cleaned_users_owned_games.csv"  
spark = SparkSession.builder.appName("CleanUsersOwnedGames").getOrCreate()

df_raw = spark.read.option("multiline", True).json(input_path)

df_exploded = df_raw.select(F.explode(F.map_entries(F.col("*"))).alias("kv")) \
    .select(
        F.col("kv.key").alias("steamid"),
        F.col("kv.value").alias("games")
    )

df_games = df_exploded.select(
    F.col("steamid"),
    F.explode("games").alias("game")
)
df_flat = df_games.select(
    "steamid",
    F.col("game.appid").alias("appid"),
    F.col("game.name").alias("name"),
    F.col("game.playtime_forever").alias("playtime_forever"),
    F.col("game.playtime_2weeks").alias("playtime_2weeks"),
    F.col("game.has_community_visible_stats").alias("has_community_visible_stats"),
    F.col("game.has_leaderboards").alias("has_leaderboards"),
    F.col("game.content_descriptorids").alias("content_descriptorids"),
    F.col("game.img_icon_url").alias("img_icon_url")
)

df_filled = df_flat.fillna({
    "playtime_forever": 0,
    "playtime_2weeks": 0,
    "has_community_visible_stats": False,
    "has_leaderboards": False,
    "content_descriptorids": "",
    "img_icon_url": "",
    "name": ""
})

df_cleaned = df_filled.withColumn(
    "content_descriptorids",
    F.when(F.col("content_descriptorids").isNotNull(),
           F.concat_ws(",", F.col("content_descriptorids")))
     .otherwise("")
)
df_cleaned = df_cleaned.withColumn(
    "name",
    F.regexp_replace(F.col("name"), r"[™®]", "")
)
df_cleaned = df_cleaned.withColumn("name", F.trim(F.col("name")))

df_cleaned.write.mode("overwrite").option("header", True).csv(output_path)
print(f"Cleaned data saved to {output_path}")
