from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("SilverToGoldSteam") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

GAMES_PATH = "s3a://spark-scripts/steam_apps_cleaned.parquet"

def get_price_segment_col(col_name):
    p = F.col(col_name).cast("float")
    return F.when(p.isNull() | F.isnan(p), "Unknown") \
            .when(p == 0, "Free") \
            .when((p > 0) & (p <= 9.99), "$1-9.99") \
            .when((p >= 10) & (p <= 19.99), "$10-19.99") \
            .when((p >= 20) & (p <= 49.99), "$20-49.99") \
            .when(p >= 50, "$50+") \
            .otherwise("Other")

def read_data(path):
    return spark.read.option("mergeSchema", "true").parquet(path)
df_games = read_data(GAMES_PATH)


# --- BUILD gold_game_facts ---
gold_game_facts_df = df_games.select(
    F.col("appid"),
    F.col("name"),
    F.col("developers").alias("developer_arr"),
    F.col("developers")[0].alias("developer"), 
    F.col("genres"),
    F.col("publishers").alias("publishers_arr"),
    F.col("initial_price"),
    F.col("final_price"),
    F.col("discount_percent"),
    get_price_segment_col("final_price").alias("price_segment"),
    F.col("release_date"),
    F.year(F.col("release_date")).alias("release_year"),
    F.col("coming_soon"),
    F.col("windows"),
    F.col("mac"),
    F.col("linux"),
    F.col("total_rec_counts").alias("total_rec"),
    F.col("achievements_total").alias("total_achievements"),
    F.col("owners"),
    F.col("positive_reviews"),
    F.col("negative_reviews"),
    F.col("avg_playtime_forever"),
    F.col("avg_playtime_2weeks"),
    F.col("median_playtime_forever"),
    F.col("median_playtime_2weeks"),
    F.col("concurrent_use"),
    F.when(F.col("movies").isNull(), 0)
     .otherwise(F.size(F.col("movies")))
     .alias("total_movies")
)


gold_game_facts_df.persist(StorageLevel.MEMORY_AND_DISK)
output_path = "s3a://spark-scripts/gold_game_fact.parquet"
gold_game_facts_df.write \
    .partitionBy("release_year") \
    .mode("overwrite") \
    .parquet(output_path)










# --- BUILD top_10_game_by_X ----
games_lite = gold_game_facts_df.select(
    F.col("appid"),
    F.col("name"),
    F.coalesce(F.col("final_price"), F.lit(0.0)).alias("price"),
    (F.coalesce(F.col("avg_playtime_forever"), F.lit(0.0)) + F.coalesce(F.col("avg_playtime_2weeks"), F.lit(0.0))).alias("avg_playtime"),
    F.coalesce(F.col("positive_reviews"), F.lit(0)).alias("positive_reviews"),
    F.coalesce(F.col("negative_reviews"), F.lit(0)).alias("negative_reviews")
)

# 2. Generic Top-K Function
# Returns a DataFrame instead of a Python list, allowing us to keep processing in Spark.
def get_top_k_df(df, metric_col, category_name, k=10):
    return df.orderBy(F.col(metric_col).desc_nulls_last()) \
             .limit(k) \
             .select(
                 F.lit(category_name).alias("metric_category"),
                 F.col("appid"),
                 F.col("name"),
                 F.col(metric_col).cast("double").alias("value") 
             )

# 3. Calculate Top K Lists (Lazy Evaluation)
# These are instant because 'games_lite' is small and cached (from previous steps)
top_price = get_top_k_df(games_lite, "price", "price")
top_playtime = get_top_k_df(games_lite, "avg_playtime", "avg_playtime")
top_positive = get_top_k_df(games_lite, "positive_reviews", "positive_reviews")
top_negative = get_top_k_df(games_lite, "negative_reviews", "negative_reviews")

# 4. Union Results
# Combine all results into one DataFrame ("Tidy Data" format).
final_analytics_df = top_price \
    .union(top_playtime) \
    .union(top_positive) \
    .union(top_negative)

# 5. Write correct dataframe
# We write 'final_analytics_df' (40 rows) instead of the massive 'gold_game_facts_df'.
output_path = "s3a://spark-scripts/gold_game_analytics.parquet"
final_analytics_df.write.mode("overwrite").parquet(output_path)









# --- BUILD genre_analytics ---
# 1. Explode, Prune, and Pre-calculate
# We create a "Lite" dataframe. This is critical because 'explode' multiplies row count.
# We don't want to carry unused columns (descriptions, movies, etc.) through the explode.
genre_df_lite = gold_game_facts_df.select(
    F.col("appid"),
    F.col("name"),
    F.col("developer"),
    F.col("release_year"),
    F.col("final_price"),
    F.col("price_segment"),
    F.explode_outer("genres").alias("genre"),
    # Pre-calculate metrics to avoid repetitive coalescing later
    (F.coalesce("avg_playtime_forever", F.lit(0)) + F.coalesce("avg_playtime_2weeks", F.lit(0))).alias("playtime_total"),
    F.coalesce("positive_reviews", F.lit(0)).alias("positive_reviews"),
    F.coalesce("negative_reviews", F.lit(0)).alias("negative_reviews")
).persist(StorageLevel.MEMORY_AND_DISK) # Cache this "skinny" exploded table

# 2. Base Aggregation (Genre Level)
genre_agg = genre_df_lite.groupBy("genre").agg(
    F.countDistinct("appid").alias("total_games"),
    F.countDistinct("developer").alias("total_developers"),
    F.avg("final_price").alias("avg_price"),
    F.avg("playtime_total").alias("avg_playtime_forever"), # Approximation based on total
    F.avg("playtime_total").alias("avg_playtime_2weeks")   # (Simplified for performance, adjust if strict separate avg needed)
)

# 3. Distributions (Trends & Segments)
# We collect lists immediately to avoid joining back huge datasets
genre_release_trend = genre_df_lite.filter(F.col("release_year").isNotNull()) \
    .groupBy("genre", "release_year").count() \
    .orderBy("genre", "release_year") \
    .groupBy("genre").agg(F.collect_list(F.struct(F.col("release_year").alias("year"), F.col("count"))).alias("release_trend"))

genre_price_dist = genre_df_lite.groupBy("genre", "price_segment").count() \
    .groupBy("genre").agg(F.collect_list(F.struct(F.col("price_segment").alias("segment"), F.col("count"))).alias("price_distribution"))

# 4. Top Developers Logic
# First, aggregate to (Genre, Developer) grain. This reduces data size massively before ranking.
dev_genre_stats = genre_df_lite.groupBy("genre", "developer").agg(
    F.countDistinct("appid").alias("num_games"),
    F.avg("playtime_total").alias("avg_playtime"),
    F.sum("positive_reviews").alias("total_positive_reviews"),
    F.sum("negative_reviews").alias("total_negative_reviews")
)

# Generic function for ranking developers per genre
def get_top_devs_per_genre(df, sort_col, output_col, k=5):
    w = Window.partitionBy("genre").orderBy(F.col(sort_col).desc_nulls_last())
    return df.select("genre", "developer", sort_col) \
        .withColumn("rn", F.row_number().over(w)) \
        .filter(F.col("rn") <= k) \
        .groupBy("genre") \
        .agg(F.collect_list(F.struct(F.col("developer"), F.col(sort_col).alias("value"))).alias(output_col))

top_devs_num_games = get_top_devs_per_genre(dev_genre_stats, "num_games", "top_devs_by_num_games")
top_devs_playtime = get_top_devs_per_genre(dev_genre_stats, "avg_playtime", "top_devs_by_avg_playtime")
top_devs_positive = get_top_devs_per_genre(dev_genre_stats, "total_positive_reviews", "top_devs_by_positive")
top_devs_negative = get_top_devs_per_genre(dev_genre_stats, "total_negative_reviews", "top_devs_by_negative")

# 5. Top Games Logic
# Generic function for ranking games per genre
def get_top_games_per_genre(df, sort_col, output_col, k=10):
    w = Window.partitionBy("genre").orderBy(F.col(sort_col).desc_nulls_last())
    # Select only columns needed for the Struct to keep shuffle small
    return df.select("genre", "appid", "name", sort_col) \
        .withColumn("rn", F.row_number().over(w)) \
        .filter(F.col("rn") <= k) \
        .groupBy("genre") \
        .agg(F.collect_list(F.struct(F.col("appid"), F.col("name"), F.col(sort_col).alias("value"))).alias(output_col))

tg_price = get_top_games_per_genre(genre_df_lite, "final_price", "top_games_by_price")
tg_playtime = get_top_games_per_genre(genre_df_lite, "playtime_total", "top_games_by_playtime")
tg_positive = get_top_games_per_genre(genre_df_lite, "positive_reviews", "top_games_by_positive")
tg_negative = get_top_games_per_genre(genre_df_lite, "negative_reviews", "top_games_by_negative")

# 6. Final Join
# Use Broadcast Joins. The sub-tables (trends, top lists) have 1 row per genre. 
# They are tiny.
gold_genre = genre_agg \
    .join(F.broadcast(genre_release_trend), on="genre", how="left") \
    .join(F.broadcast(genre_price_dist), on="genre", how="left") \
    .join(F.broadcast(top_devs_num_games), on="genre", how="left") \
    .join(F.broadcast(top_devs_playtime), on="genre", how="left") \
    .join(F.broadcast(top_devs_positive), on="genre", how="left") \
    .join(F.broadcast(top_devs_negative), on="genre", how="left") \
    .join(F.broadcast(tg_price), on="genre", how="left") \
    .join(F.broadcast(tg_playtime), on="genre", how="left") \
    .join(F.broadcast(tg_positive), on="genre", how="left") \
    .join(F.broadcast(tg_negative), on="genre", how="left")

output_path = "s3a://spark-scripts/gold_genre.parquet"
gold_genre.write.mode("overwrite").parquet(output_path)

# Cleanup
genre_df_lite.unpersist()







# --- BUILD gold developer analytics ---

# 1. Column Pruning & Cleaning
# We select ONLY the columns needed for metrics and ranking. 
# We also handle NULLs here once, so downstream aggregations are faster.
dev_df_lite = gold_game_facts_df.select(
    F.col("developer").alias("developer_single"),
    "appid",
    "name",
    "final_price",
    "price_segment",
    "avg_playtime_forever",
    "avg_playtime_2weeks",
    F.coalesce(F.col("positive_reviews"), F.lit(0)).alias("positive_reviews"),
    F.coalesce(F.col("negative_reviews"), F.lit(0)).alias("negative_reviews")
).filter(F.col("developer_single").isNotNull())

# 2. Cache the Lite Dataframe
# We use this specific dataframe for 6 different parallel operations.
dev_df_lite.cache()

# --- Operation A: Main Metrics Aggregation ---
dev_agg = dev_df_lite.groupBy("developer_single").agg(
    F.countDistinct("appid").alias("total_games"),
    F.sum(F.when(F.col("final_price") == 0, 1).otherwise(0)).alias("total_free_games"),
    F.avg("final_price").alias("avg_price"),
    F.avg("avg_playtime_forever").alias("avg_playtime_forever"),
    F.avg("avg_playtime_2weeks").alias("avg_playtime_2weeks"),
    F.sum("positive_reviews").alias("total_positive_reviews"),
    F.sum("negative_reviews").alias("total_negative_reviews"),
    F.avg("positive_reviews").alias("avg_positive_reviews"),
    F.avg("negative_reviews").alias("avg_negative_reviews")
)

# --- Operation B: Price Distribution ---
dev_price_dist = dev_df_lite.groupBy("developer_single", "price_segment").count() \
    .groupBy("developer_single") \
    .agg(F.collect_list(F.struct(F.col("price_segment").alias("segment"), F.col("count"))).alias("price_distribution"))

# --- Operation C: Top K Games (Refactored) ---
def get_top_games_per_dev(df, sort_col, output_alias, k=10):
    # Because 'df' is now our cached 'dev_df_lite', these Window operations 
    # are much lighter (shuffling less data).
    w = Window.partitionBy("developer_single").orderBy(F.col(sort_col).desc_nulls_last())
    
    return df.select("developer_single", "appid", "name", sort_col) \
        .withColumn("rn", F.row_number().over(w)) \
        .filter(F.col("rn") <= k) \
        .groupBy("developer_single").agg(
            F.collect_list(F.struct(
                F.col("appid"), 
                F.col("name"), 
                F.col(sort_col).alias("value")
            )).alias(output_alias)
        )

dev_tg_price = get_top_games_per_dev(dev_df_lite, "final_price", "top_games_by_price")
dev_tg_playtime = get_top_games_per_dev(dev_df_lite, "avg_playtime_forever", "top_games_by_playtime")
dev_tg_positive = get_top_games_per_dev(dev_df_lite, "positive_reviews", "top_games_by_positive")
dev_tg_negative = get_top_games_per_dev(dev_df_lite, "negative_reviews", "top_games_by_negative")

# --- Join & Write ---
# Using standard left joins. Since 'dev_agg' contains the unique list of developers,
# we join everything against it.
dev_results = dev_agg \
    .join(dev_price_dist, on="developer_single", how="left") \
    .join(dev_tg_price, on="developer_single", how="left") \
    .join(dev_tg_playtime, on="developer_single", how="left") \
    .join(dev_tg_positive, on="developer_single", how="left") \
    .join(dev_tg_negative, on="developer_single", how="left") \
    .withColumnRenamed("developer_single", "developer")

output_path = "s3a://spark-scripts/gold_dev.parquet"
dev_results.write.mode("overwrite").parquet(output_path)

# Cleanup
dev_df_lite.unpersist()








#-- BUILD gold price segment anlytics ---

df_lite = gold_game_facts_df.select(
    "price_segment", 
    "appid", 
    "name", 
    "final_price", 
    "avg_playtime_forever", 
    "avg_playtime_2weeks", 
    F.coalesce(F.col("positive_reviews"), F.lit(0)).alias("positive_reviews"),
    F.coalesce(F.col("negative_reviews"), F.lit(0)).alias("negative_reviews")
).withColumn(
    "playtime_total", F.col("avg_playtime_forever") + F.col("avg_playtime_2weeks")
)
df_lite.cache()

price_seg_agg = df_lite.groupBy("price_segment").agg(
    F.countDistinct("appid").alias("total_games"),
    F.avg("final_price").alias("avg_price"),
    F.avg("avg_playtime_forever").alias("avg_playtime_forever"),
    F.avg("avg_playtime_2weeks").alias("avg_playtime_2weeks"),
    F.sum("positive_reviews").alias("total_positive_reviews"),
    F.sum("negative_reviews").alias("total_negative_reviews"),
    F.avg("positive_reviews").alias("avg_positive_reviews"),
    F.avg("negative_reviews").alias("avg_negative_reviews")
)

# --- Top K Helper Function ---
def get_top_k_by_segment(df, metric_col, output_alias, k=10):
    # We only select the columns needed for the struct to save memory during the shuffle
    w = Window.partitionBy("price_segment").orderBy(F.col(metric_col).desc_nulls_last())
    
    return df.select("price_segment", "appid", "name", metric_col) \
        .withColumn("rn", F.row_number().over(w)) \
        .filter(F.col("rn") <= k) \
        .groupBy("price_segment") \
        .agg(F.collect_list(
            F.struct(F.col("appid"), F.col("name"), F.col(metric_col).alias("value"))
        ).alias(output_alias))

# --- Calculate Rankings ---
top_playtime = get_top_k_by_segment(df_lite, "playtime_total", "top_10_by_playtime")
top_positive = get_top_k_by_segment(df_lite, "positive_reviews", "top_10_by_positive")
top_negative = get_top_k_by_segment(df_lite, "negative_reviews", "top_10_by_negative")

# --- Join & Write ---
# Use Broadcast Joins. The Top K tables are very small (rows = number of price segments).
# This avoids shuffling the 'price_seg_agg' table.
price_segment_results = price_seg_agg \
    .join(F.broadcast(top_playtime), on="price_segment", how="left") \
    .join(F.broadcast(top_positive), on="price_segment", how="left") \
    .join(F.broadcast(top_negative), on="price_segment", how="left")

output_path = "s3a://spark-scripts/gold_price_segment.parquet"
price_segment_results.write.mode("overwrite").parquet(output_path)

# Cleanup
df_lite.unpersist()








#--- BUILD release_trend_analytics ---

# 1. Column Pruning & Caching
# Optimization: Select strictly necessary columns to minimize memory usage.
# Filter early to reduce dataset size.
release_df = gold_game_facts_df \
    .select("release_year", "appid", "genres", "developer") \
    .filter((F.col("release_year") >= 2015) & (F.col("release_year") <= 2025))

# Optimization: Cache because this DF is scanned 3 times below.
release_df.cache()

# --- Branch 1: Total Games ---
total_games_per_year = release_df.groupBy("release_year") \
    .agg(F.countDistinct("appid").alias("total_games"))

# --- Branch 2: Top Genres ---
# Optimization: Use .select() to drop 'developer' column before this specific shuffle.
# Logic: Retain explode_outer and countDistinct to match original logic exactly.
genre_year_count = release_df \
    .select("release_year", "appid", F.explode_outer("genres").alias("genre")) \
    .groupBy("release_year", "genre") \
    .agg(F.countDistinct("appid").alias("count"))

top_genres_per_year = genre_year_count \
    .withColumn("rn", F.row_number().over(Window.partitionBy("release_year").orderBy(F.col("count").desc_nulls_last()))) \
    .filter(F.col("rn") <= 10) \
    .groupBy("release_year") \
    .agg(F.collect_list(F.struct(F.col("genre"), F.col("count"))).alias("top_10_genres"))

# --- Branch 3: Top Developers ---
# Optimization: Group directly on the pruned cached DF.
dev_year_count = release_df.groupBy("release_year", "developer") \
    .agg(F.countDistinct("appid").alias("count"))

top_devs_per_year = dev_year_count \
    .withColumn("rn", F.row_number().over(Window.partitionBy("release_year").orderBy(F.col("count").desc_nulls_last()))) \
    .filter(F.col("rn") <= 10) \
    .groupBy("release_year") \
    .agg(F.collect_list(F.struct(F.col("developer"), F.col("count"))).alias("top_10_developers"))

# Optimization: Use Broadcast joins. The left/right tables are tiny (11 rows each).
release_trend_results = total_games_per_year \
    .join(F.broadcast(top_genres_per_year), on="release_year", how="left") \
    .join(F.broadcast(top_devs_per_year), on="release_year", how="left") \
    .orderBy("release_year")

output_path = "s3a://spark-scripts/gold_release_trend.parquet"
release_trend_results.write.mode("overwrite").parquet(output_path)

release_df.unpersist()





#--- BUILD gold player in 30 mins
HISTORY_PATH = "s3a://spark-scripts/historical_player_counts_monthly.parquet"
OUTPUT_HISTORY = "s3a://spark-scripts/gold_player_count.parquet"
history = spark.read.json(HISTORY_PATH).select(
    F.col("event_timestamp"),
    F.col("app_id"),
    F.col("player_count"),
    F.col("peak_players_monthly")
)

gold_player_count = history.groupBy(
    F.col("app_id"),
    F.window(F.col("event_timestamp"), "30 minutes")
).agg(
    F.max("player_count").alias("max_players"),
    F.round(F.avg("player_count"), 2).alias("avg_players"),
    F.max("peak_players_monthly").alias("peak_players_monthly")
)
output_df = gold_player_count.orderBy("app_id", "window.start")
output_df.write.mode("overwrite").parquet("path/to/gold_player_count.parquet")






#--- BUILD gold review in 1 hours
REVIEWS_INPUT_PATH = "s3a://spark-scripts/top10_review_cleaned/*.parquet"
REVIEWS_OUTPUT_PATH = "s3a://spark-scripts/gold_reviews.parquet"

df_reviews = spark.read.parquet(REVIEWS_INPUT_PATH) \
    .withColumn(
        "app_id", 
        F.regexp_extract(F.input_file_name(), r"\/(\d+)\.parquet$", 1)
    ) \
    .filter(F.col("app_id") != "") 

df_lite = df_reviews.select(
    F.col("app_id"),
    F.col("timestamp_created").cast(T.TimestampType()), 
    F.col("voted_up"),
    F.col("weighted_vote_score")
)

gold_agg = df_lite.groupBy(
    F.col("app_id"),
    F.window(F.col("timestamp_created"), "1 hour").alias("window")
).agg(
    F.count("*").alias("total_reviews"),
    F.sum(F.when(F.col("voted_up") == True, 1).otherwise(0)).alias("positive_count"),
    F.sum(F.when(F.col("voted_up") == False, 1).otherwise(0)).alias("negative_count"),
    F.avg("weighted_vote_score").alias("avg_quality")
)

final_df = gold_agg.withColumn(
    "negative_ratio", 
    F.round(F.col("negative_count") / F.col("total_reviews"), 4)
        ).withColumn(
            "positive_ratio", 
            F.round(F.col("positive_count") / F.col("total_reviews"), 4)
        ).select(
            "app_id",
            "total_reviews",
            "negative_count",
            "positive_count",
            F.round("avg_quality", 4).alias("avg_quality"),
            "negative_ratio",
            "positive_ratio",
            "window"
        )
final_df.write \
    .partitionBy("app_id") \
    .mode("overwrite") \
    .parquet(REVIEWS_OUTPUT_PATH )



