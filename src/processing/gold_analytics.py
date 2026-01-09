import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel

CHECKPOINT_BASE = (
    "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000/checkpoints/gold"
)
MONGO_URI = "mongodb://admin:password@my-mongodb.database.svc:27017/steam_analytics?authSource=steam_analytics"
WAREHOUSE_PATH = (
    "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000/iceberg_data"
)
NESSIE_URI = "http://nessie.nessie-ns.svc:19120/api/v1"


def get_spark_session(app_name="GoldKappaStreamOptimized"):
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE_PATH)
        .getOrCreate()
    )


def write_dual_sink(batch_df, batch_id, iceberg_table, mongo_collection):
    if batch_df.isEmpty():
        return
    # Write to Iceberg
    batch_df.write.format("iceberg").mode("append").saveAsTable(iceberg_table)
    # Write to Mongo
    batch_df.write.format("mongodb").mode("append").option(
        "connection.uri", MONGO_URI
    ).option("database", "steam_analytics").option(
        "collection", mongo_collection
    ).save()


def get_price_segment_col(col_name):
    p = F.col(col_name).cast("float")
    return (
        F.when(p.isNull() | F.isnan(p), "Unknown")
        .when(p == 0, "Free")
        .when((p > 0) & (p <= 9.99), "10-19.99")
        .when((p >= 20) & (p <= 49.99), "20-49.99")
        .when(p >= 50, "50+")
        .otherwise("Other")
    )


def transform_game_facts(df_games):
    """
    Standardizes raw data into facts. No aggregations here.
    """
    return df_games.select(
        F.col("appid"),
        F.col("name"),
        F.col("developers")[0].alias("developer"),
        F.col("genres"),
        F.col("initial_price"),
        F.col("final_price"),
        F.col("discount_percent"),
        get_price_segment_col("final_price").alias("price_segment"),
        F.year(F.col("release_date")).alias("release_year"),
        F.col("release_date"),
        (
            F.coalesce(F.col("avg_playtime_forever"), F.lit(0))
            + F.coalesce(F.col("avg_playtime_2weeks"), F.lit(0))
        ).alias("playtime_total"),
        F.coalesce(F.col("positive_reviews"), F.lit(0)).alias("positive_reviews"),
        F.coalesce(F.col("negative_reviews"), F.lit(0)).alias("negative_reviews"),
    )


def calculate_top_k_in_group(metric_col, name_col, id_col, alias_name, k=5):
    """
    Returns a Column expression that:
    1. Creates a Struct with a NEGATIVE sort key (to emulate DESC sort).
    2. Collects all rows in the group into a list.
    3. Sorts the array (uses the negative key).
    4. Slices the Top K.
    5. Transforms the result to remove the negative sort key for clean output.
    """
    struct_col = F.struct(
        (-F.col(metric_col)).alias("sort_key"),
        F.col(metric_col).alias("value"),
        F.col(name_col).alias("name"),
        F.col(id_col).alias("id"),
    )

    def clean_struct(x):
        return F.struct(
            x["value"].alias("value"), x["name"].alias("name"), x["id"].alias("id")
        )

    return F.transform(
        F.slice(F.sort_array(F.collect_list(struct_col)), 1, k), clean_struct
    ).alias(alias_name)


def transform_genre_analytics_optimized(df_facts):
    df = df_facts.select(
        F.explode_outer("genres").alias("genre"),
        "appid",
        "name",
        "developer",
        "final_price",
        "playtime_total",
        "positive_reviews",
        "negative_reviews",
    )

    main_agg = df.groupBy("genre").agg(
        F.countDistinct("appid").alias("total_games"),
        F.countDistinct("developer").alias("total_developers"),
        F.avg("final_price").alias("avg_price"),
        calculate_top_k_in_group("final_price", "name", "appid", "top_games_by_price"),
        calculate_top_k_in_group(
            "playtime_total", "name", "appid", "top_games_by_playtime"
        ),
        calculate_top_k_in_group(
            "positive_reviews", "name", "appid", "top_games_by_positive"
        ),
        calculate_top_k_in_group(
            "negative_reviews", "name", "appid", "top_games_by_negative"
        ),
    )

    dev_stats = df.groupBy("genre", "developer").agg(
        F.count("appid").alias("num_games"),
        F.sum("positive_reviews").alias("total_pos"),
    )
    dev_agg = dev_stats.groupBy("genre").agg(
        calculate_top_k_in_group(
            "num_games", "developer", "developer", "top_devs_by_games"
        ),
        calculate_top_k_in_group(
            "total_pos", "developer", "developer", "top_devs_by_pos"
        ),
    )

    return main_agg.join(dev_agg, "genre", "left")


def transform_developer_analytics_optimized(df_facts):
    return (
        df_facts.filter(F.col("developer").isNotNull())
        .groupBy("developer")
        .agg(
            F.countDistinct("appid").alias("total_games"),
            F.sum(F.when(F.col("final_price") == 0, 1).otherwise(0)).alias(
                "total_free_games"
            ),
            F.avg("final_price").alias("avg_price"),
            F.sum("positive_reviews").alias("total_positive_reviews"),
            calculate_top_k_in_group(
                "final_price", "name", "appid", "top_games_by_price"
            ),
            calculate_top_k_in_group(
                "playtime_total", "name", "appid", "top_games_by_playtime"
            ),
            calculate_top_k_in_group(
                "positive_reviews", "name", "appid", "top_games_by_positive"
            ),
        )
    )


def transform_price_analytics_optimized(df_facts):
    return df_facts.groupBy("price_segment").agg(
        F.countDistinct("appid").alias("total_games"),
        F.avg("playtime_total").alias("avg_playtime"),
        F.avg("positive_reviews").alias("avg_positive_reviews"),
        calculate_top_k_in_group(
            "playtime_total", "name", "appid", "top_games_by_playtime"
        ),
        calculate_top_k_in_group(
            "positive_reviews", "name", "appid", "top_games_by_positive"
        ),
    )


def transform_release_trend_optimized(df_facts):
    df_lite = df_facts.filter(
        (F.col("release_year") >= 2015) & (F.col("release_year") <= 2025)
    )

    return df_lite.groupBy("release_year").agg(
        F.countDistinct("appid").alias("total_games"),
        F.collect_set("developer").alias("developers_active"),
    )


def process_gold_master_batch(raw_df, batch_id):
    if raw_df.isEmpty():
        return

    facts_batch = transform_game_facts(raw_df)

    facts_batch.persist(StorageLevel.MEMORY_AND_DISK)

    tasks = [
        (lambda df: df, "nessie.gold.game_fact", "game_fact"),
        (transform_genre_analytics_optimized, "nessie.gold.game_genre", "game_genre"),
        (transform_developer_analytics_optimized, "nessie.gold.game_dev", "game_dev"),
        (
            transform_price_analytics_optimized,
            "nessie.gold.price_segment",
            "price_segment",
        ),
        (
            transform_release_trend_optimized,
            "nessie.gold.release_trend",
            "release_trend",
        ),
    ]

    try:
        for transform_func, iceberg_tbl, mongo_coll in tasks:
            out_df = transform_func(facts_batch)
            write_dual_sink(out_df, batch_id, iceberg_tbl, mongo_coll)

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
    finally:
        facts_batch.unpersist()


def run_pipeline(games_df):
    return (
        games_df.writeStream.trigger(processingTime="5 minutes")
        .foreachBatch(process_gold_master_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_master_opt")
        .start()
    )


def main():
    spark = get_spark_session()
    games_df = spark.readStream.format("iceberg").load("nessie.silver.steam_games")

    q_gold = run_pipeline(games_df)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
