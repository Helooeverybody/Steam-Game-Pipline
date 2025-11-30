from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("SilverToGoldSteam").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

GAMES_PATH = "s3a://spark-scripts/steam_apps_cleaned.parquet"

#--- BUILD utilities function --
def price_segment(p):
    if p is None:
        return "Unknown"
    try:
        p = float(p)
    except:
        return "Unknown"
    if p == 0:
        return "Free"
    if 0 < p <= 9.99:
        return "$1-9.99"
    if 10 <= p <= 19.99:
        return "$10-19.99"
    if 20 <= p <= 49.99:
        return "$20-49.99"
    if p >= 50:
        return "$50+"
    return "Other"
def first_or_null(arr):
    if arr is None:
        return None
    try:
        if isinstance(arr, (list, tuple)) and len(arr) > 0:
            return arr[0]
        return arr
    except:
        return None
def arr_size_safe(a):
    return len(a) if a is not None else 0
arr_size_udf = F.udf(arr_size_safe, T.IntegerType())
first_or_null_udf = F.udf(first_or_null, T.StringType())
price_segment_udf = F.udf(price_segment, T.StringType())

def read_maybe_parquet(path):
    import os
    try:
        return spark.read.parquet(path)
    except Exception:
        try:
            return spark.read.option("multiline", True).json(path)
        except Exception:
            return spark.read.option("header", True).csv(path)

df_games = read_maybe_parquet(GAMES_PATH)

#--- BUILD gold_game_facts ---
gold_game_facts_df = df_games.select(
    F.col("appid"),
    F.col("name"),
    F.col("developers").alias("developer_arr"),
    first_or_null_udf(F.col("developers")).alias("developer"),
    F.col("genres"),
    F.col("publishers").alias("publishers_arr"),
    F.col("initial_price"),
    F.col("final_price"),
    F.col("discount_percent"),
    price_segment_udf(F.col("final_price")).alias("price_segment"),
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
    F.size(F.col("movies")).alias("total_movies")
    
)
output_path = "s3a://spark-scripts/gold_game_fact.parquet"
gold_game_facts_df.write.mode("overwrite").parquet(output_path)

#--- BUILD top_10_game_by_X ----
def top_k(df, col, k = 10, asc = False):
    if asc:
        ordered = df.orderBy(F.col(col).asc_nulls_last())
    else:
        ordered = df.orderBy(F.col(col).desc_nulls_last())
    return ordered.select("appid", "name", col).limit(k)
def df_to_records_list(df):
    recs = df.collect()
    return [{"appid": r[0], "name": r[1], "value": (r[2] if r[2] is not None else None)} for r in recs]

games_for_analytics = gold_game_facts_df\
    .withColumn("avg_playtime", F.coalesce(F.col("avg_playtime_forever"), F.lit(0.0)) + F.coalesce(F.col("avg_playtime_2weeks"), F.lit(0.0)))\
    .withColumn("price", F.coalesce(F.col("final_price"), F.lit(0.0))) \
    .withColumn("positive_reviews", F.coalesce(F.col("positive_reviews"), F.lit(0))) \
    .withColumn("negative_reviews", F.coalesce(F.col("negative_reviews"), F.lit(0))) 

top_price = top_k(games_for_analytics, "price", 10)
top_playtime = top_k(games_for_analytics, "avg_playtime", 10)
top_positive = top_k(games_for_analytics, "positive_reviews", 10)
top_negative = top_k(games_for_analytics, "negative_reviews", 10)

gold_game_analytics = {
    "price": df_to_records_list(top_price),
    "avg_playtime": df_to_records_list(top_playtime),
    "positive_reviews": df_to_records_list(top_positive),
    "negative_reviews": df_to_records_list(top_negative),
}
output_path = "s3a://spark-scripts/gold_game_analytics.parquet"
gold_game_facts_df.write.mode("overwrite").parquet(output_path)


#--- BUILD genre_anlytics ---

games_with_genre = gold_game_facts_df.withColumn("genre", F.explode_outer('genres'))

genre_agg = games_with_genre.groupBy("genre").agg(
    F.countDistinct("appid").alias("total_games"),
    F.countDistinct("developer").alias("total_developers"),
    F.avg("final_price").alias("avg_price"),
    F.avg("avg_playtime_forever").alias("avg_playtime_forever"),
    
    F.avg("avg_playtime_2weeks").alias("avg_playtime_2weeks")
)

#released_trend
genre_release_trend = games_with_genre.groupBy("genre", "release_year").agg(F.count("*").alias("count"))\
                         .filter(F.col("release_year").isNotNull()).orderBy("genre", "release_year")
#price_distribution
genre_price_dist = games_with_genre.groupBy("genre", "price_segment").agg(F.count("*").alias("count"))

#top5developers: criteria (num_games,avg_playtime, total_positive_reviews, total_negative_reviews)
dev_genre_agg = games_with_genre.groupBy("genre", "developer").agg(
    F.countDistinct("appid").alias("num_games"),
    (F.avg("avg_playtime_forever") + F.avg("avg_playtime_2weeks")).alias("avg_playtime"),
    F.sum(F.coalesce("positive_reviews", F.lit(0))).alias("total_positive_reviews"),
    F.sum(F.coalesce("negative_reviews", F.lit(0))).alias("total_negative_reviews")
)
def top_developers(col,k):
    w = Window.partitionBy("genre").orderBy(F.col(col).desc_nulls_last())
    ranked = dev_genre_agg.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= k)
    grouped = ranked.groupBy("genre").agg(
        F.collect_list(F.struct(F.col("developer").alias("developer"), F.col(col).alias("value"))).alias(f"top_devs_by_{col}")
    )
    return grouped
top_devs_by_num_games = top_developers("num_games", 5)
top_devs_by_avg_playtime = top_developers("avg_playtime", 5)
top_devs_by_positive = top_developers("total_positive_reviews", 5)
top_devs_by_negative = top_developers("total_negative_reviews", 5)

#top10gamesbyX
games_with_metrics = games_with_genre.withColumn("playtime_total", F.col("avg_playtime_forever") + F.col("avg_playtime_2weeks"))
def top_games(col, k=10):
    w = Window.partitionBy("genre").orderBy(F.col(col).desc_nulls_last())
    ranked = games_with_metrics.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= k)
    grouped = ranked.groupBy("genre").agg(
        F.collect_list(F.struct(F.col("appid").alias("appid"), F.col("name").alias("name"), F.col(col).alias("value")))\
            .alias(f"top_games_by_{col}")
    )
    return grouped
tg_price = top_games("final_price", 10)
tg_playtime = top_games("playtime_total", 10)
tg_positive = top_games("positive_reviews", 10)
tg_negative = top_games("negative_reviews", 10)

#----
gold_genre = genre_agg.join(
    genre_release_trend.groupBy("genre").agg(
        F.collect_list(F.struct(F.col("release_year").alias("year"), F.col("count").alias("count"))).alias("release_trend")
        ), on="genre", how="left"
    ).join(
        genre_price_dist.groupBy("genre").agg(F.collect_list(F.struct(F.col("price_segment").alias("segment"), F.col("count").alias("count")))\
                                              .alias("price_distribution")
        ), on ="genre", how = "left"
    ).join(top_devs_by_num_games, on="genre", how="left") \
     .join(top_devs_by_avg_playtime, on="genre", how="left") \
     .join(top_devs_by_positive, on="genre", how="left") \
     .join(top_devs_by_negative, on="genre", how="left") \
     .join(tg_price, on="genre", how="left") \
     .join(tg_playtime, on="genre", how="left") \
     .join(tg_positive, on="genre", how="left") \
     .join(tg_negative, on="genre", how="left") 

output_path = "s3a://spark-scripts/gold_genre.parquet"
gold_genre.write.mode("overwrite").parquet(output_path)

#--- BUILD gold developer analytics ---
games_with_dev = gold_game_facts_df.withColumn("developer_single", F.col("developer"))
dev_agg = games_with_dev.groupBy("developer_single").agg(
    F.countDistinct("appid").alias("total_games"),
    F.sum(F.when(F.col("final_price") == 0, 1).otherwise(0)).alias("total_free_games"),
    F.avg("final_price").alias("avg_price"),
    F.avg("avg_playtime_forever").alias("avg_playtime_forever"),
    F.avg("avg_playtime_2weeks").alias("avg_playtime_2weeks"),
    F.sum(F.coalesce("positive_reviews", F.lit(0))).alias("total_positive_reviews"),
    F.sum(F.coalesce("negative_reviews", F.lit(0))).alias("total_negative_reviews"),
    F.avg(F.coalesce("positive_reviews", F.lit(0))).alias("avg_positive_reviews"),
    F.avg(F.coalesce("negative_reviews", F.lit(0))).alias("avg_negative_reviews")
)
# price distribution per developer
dev_price_dist = games_with_dev.groupBy("developer_single", "price_segment").agg(F.count("*").alias("count")) \
    .groupBy("developer_single").agg(F.collect_list(F.struct(F.col("price_segment").alias("segment"), F.col("count").alias("count"))).alias("price_distribution"))
# top games per developer by metrics
def top_games_per_developer(col, k=10):
    w = Window.partitionBy("developer_single").orderBy(F.col(col).desc_nulls_last())
    ranked = games_with_dev.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= k)
    grouped = ranked.groupBy("developer_single").agg(
        F.collect_list(F.struct(F.col("appid").alias("appid"), F.col("name").alias("name"), F.col(col).alias("value"))).alias(f"top_games_by_{col}")
    )
    return grouped

dev_tg_price = top_games_per_developer("final_price", 10)
dev_tg_playtime = top_games_per_developer("avg_playtime_forever", 10)  # using forever as prox
dev_tg_positive = top_games_per_developer("positive_reviews", 10)
dev_tg_negative = top_games_per_developer("negative_reviews", 10)



dev_results = dev_agg.join(dev_price_dist, dev_agg.developer_single == dev_price_dist.developer_single, how="left") \
    .drop(dev_price_dist.developer_single) \
    .join(dev_tg_price, on="developer_single", how="left") \
    .join(dev_tg_playtime, on="developer_single", how="left") \
    .join(dev_tg_positive, on="developer_single", how="left") \
    .join(dev_tg_negative, on="developer_single", how="left") \
  

gold_dev = dev_results.withColumnRenamed("developer_single", "developer")

output_path = "s3a://spark-scripts/gold_dev.parquet"
gold_dev.write.mode("overwrite").parquet(output_path)


#-- BUILD gold price segment anlytics ---
price_seg_agg = gold_game_facts_df.groupBy("price_segment").agg(
    F.countDistinct("appid").alias("total_games"),
    F.avg("final_price").alias("avg_price"),
    F.avg("avg_playtime_forever").alias("avg_playtime_forever"),
    F.avg("avg_playtime_2weeks").alias("avg_playtime_2weeks"),
    F.sum(F.coalesce("positive_reviews", F.lit(0))).alias("total_positive_reviews"),
    F.sum(F.coalesce("negative_reviews", F.lit(0))).alias("total_negative_reviews"),
    F.avg(F.coalesce("positive_reviews", F.lit(0))).alias("avg_positive_reviews"),
    F.avg(F.coalesce("negative_reviews", F.lit(0))).alias("avg_negative_reviews")
)

w_seg = Window.partitionBy("price_segment").orderBy(F.col("playtime_total").desc_nulls_last())
games_with_playtime = gold_game_facts_df.withColumn("playtime_total", F.col("avg_playtime_forever") + F.col("avg_playtime_2weeks"))

top_games_by_seg_playtime = games_with_playtime\
    .withColumn("rn", F.row_number().over(Window.partitionBy("price_segment")\
    .orderBy(F.col("playtime_total").desc_nulls_last()))) \
    .filter(F.col("rn") <= 10) \
    .groupBy("price_segment") \
    .agg(F.collect_list(F.struct(F.col("appid"), F.col("name"), F.col("playtime_total").alias("value"))).alias("top_10_by_playtime"))

top_games_by_seg_positive = games_with_playtime\
    .withColumn("rn", F.row_number().over(Window.partitionBy("price_segment")\
                .orderBy(F.col("positive_reviews").desc_nulls_last()))) \
    .filter(F.col("rn") <= 10) \
    .groupBy("price_segment").agg(F.collect_list(F.struct(F.col("appid"), F.col("name"), F.col("positive_reviews").alias("value")))\
                                              .alias("top_10_by_positive"))

top_games_by_seg_negative = games_with_playtime\
    .withColumn("rn", F.row_number().over(Window.partitionBy("price_segment")\
                    .orderBy(F.col("negative_reviews").desc_nulls_last()))) \
    .filter(F.col("rn") <= 10) \
    .groupBy("price_segment").agg(F.collect_list(F.struct(F.col("appid"), F.col("name"), F.col("negative_reviews").alias("value")))\
                                                  .alias("top_10_by_negative"))


price_segment_results = price_seg_agg \
    .join(top_games_by_seg_playtime, on="price_segment", how="left") \
    .join(top_games_by_seg_positive, on="price_segment", how="left") \
    .join(top_games_by_seg_negative, on="price_segment", how="left") \
   

output_path = "s3a://spark-scripts/gold_price_segment.parquet"
price_segment_results.write.mode("overwrite").parquet(output_path)



#--- BUILD release_trend_analytics ---
release_df = gold_game_facts_df.filter((F.col("release_year") >= 2015) & (F.col("release_year") <= 2025))
# total games per year
total_games_per_year = release_df.groupBy("release_year").agg(F.countDistinct("appid").alias("total_games"))
# top 10 genres per year
genre_year_count = release_df.withColumn("genre", F.explode_outer(F.col("genres"))) \
    .groupBy("release_year", "genre").agg(F.countDistinct("appid").alias("count"))
top_genres_per_year = genre_year_count.withColumn("rn", F.row_number().over(Window.partitionBy("release_year").orderBy(F.col("count").desc_nulls_last()))) \
    .filter(F.col("rn") <= 10) \
    .groupBy("release_year") \
    .agg(F.collect_list(F.struct(F.col("genre"), F.col("count"))).alias("top_10_genres"))

dev_year_count = release_df.groupBy("release_year", "developer").agg(F.countDistinct("appid").alias("count"))
top_devs_per_year = dev_year_count.withColumn("rn", F.row_number().over(Window.partitionBy("release_year").orderBy(F.col("count").desc_nulls_last()))) \
    .filter(F.col("rn") <= 10) \
    .groupBy("release_year") \
    .agg(F.collect_list(F.struct(F.col("developer"), F.col("count"))).alias("top_10_developers"))

release_trend_results = total_games_per_year.join(top_genres_per_year, on="release_year", how="left") \
    .join(top_devs_per_year, on="release_year", how="left") \
    .orderBy("release_year")

output_path = "s3a://spark-scripts/release_trend.parquet"
release_trend_results.write.mode("overwrite").parquet(output_path)
