from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
    StructType,
    StructField,
)
import re

spark = (
    SparkSession.builder.appName("SteamGameCleanerStream")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    .config("spark.sql.catalog.nessie.uri", "http://nessie.nessie-ns.svc:19120/api/v1")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.authentication.type", "NONE")
    .config(
        "spark.sql.catalog.nessie.warehouse",
        "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000/iceberg_data",
    )
    .getOrCreate()
)

namenode_url = "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000"
checkpoint_path = f"{namenode_url}/checkpoints/steam_clean_job"


RATING_TO_AGE = {
    "ec": 3,
    "e": 6,
    "e10+": 10,
    "t": 13,
    "m": 17,
    "ao": 18,
    "3": 3,
    "7": 7,
    "12": 12,
    "16": 16,
    "18": 18,
    "all": 0,
    "l": 0,
    "g": 0,
    "u": 0,
    "a": 0,
    "pg": 10,
    "r13": 13,
    "r15": 15,
    "ma15": 15,
    "nc16": 16,
    "r16": 16,
    "r18": 18,
    "m18": 18,
    "b": 12,
    "c": 15,
    "d": 17,
    "z": 18,
}


def clean_ratings(game):
    if game is None:
        return ""
    ratings = getattr(game, "ratings", None)
    if not ratings:
        return ""

    formatted_regions = []
    for region, info in ratings.items():
        if info is None:
            continue
        req_age_raw = info["required_age"]
        rating_raw = info["rating"] or ""
        desc_raw = info["descriptors"] or ""

        if req_age_raw is None or str(req_age_raw).strip() == "":
            age = RATING_TO_AGE.get(str(rating_raw).lower().strip(), 0)
        else:
            try:
                age = int(req_age_raw)
            except ValueError:
                age = RATING_TO_AGE.get(str(req_age_raw).lower().strip(), 0)

        txt = str(desc_raw).replace("<br>", "; ").replace("<li>", "; ")
        descriptors = re.sub(r"<[^>]+>", "", txt).strip()
        formatted_regions.append(
            f"{region}: (required_age={age}, descriptors={descriptors})"
        )
    return " | ".join(formatted_regions)


clean_ratings_udf = F.udf(clean_ratings, StringType())


def clean_text_native(col_name):
    c = F.col(col_name)
    c = F.regexp_replace(c, r"<(br|li)\s*/?>", "; ")
    c = F.regexp_replace(c, r"<[^>]+>", "")
    c = F.regexp_replace(c, r"&nbsp;", " ")
    c = F.regexp_replace(c, r"&amp;", "&")
    c = F.regexp_replace(c, r"©", "(c)")
    c = F.regexp_replace(c, r"[®™]", "")
    c = F.regexp_replace(c, r"[–—]", "-")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.regexp_replace(c, r"\s*([.,;:!?])\s*", r"\1 ")
    c = F.regexp_replace(c, r"\s*-\s*", " - ")
    c = F.trim(c)
    return F.regexp_replace(c, r"^;|;$", "")


def parse_date_native(col_name):
    month_map_regex = {
        "janv.|ene.": "Jan",
        "févr.|febr.": "Feb",
        "mars|märz": "Mar",
        "avr.|abr.": "Apr",
        "mai": "May",
        "juin": "Jun",
        "juil.": "Jul",
        "août|ago.": "Aug",
        "sept.": "Sep",
        "oct.|okt.": "Oct",
        "nov.": "Nov",
        "déc.|dic.|dez.": "Dec",
    }
    c = F.col(col_name)
    for pattern, replacement in month_map_regex.items():
        c = F.regexp_replace(c, f"(?i){pattern}", replacement)
    return F.coalesce(
        F.to_date(c, "d MMMM, yyyy"),
        F.to_date(c, "MMMM d, yyyy"),
        F.to_date(c, "d MMMM yyyy"),
        F.to_date(c, "MMMM yyyy"),
        F.to_date(c, "yyyy"),
    )


def array_clean_native(col_name):
    return F.expr(
        f"filter(transform({col_name}, x -> trim(x)), x -> x != '' and x is not null)"
    )


full_game_schema = StructType(
    [
        StructField("steam_appid", LongType(), True),
        StructField("type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("required_age", StringType(), True),
        StructField("is_free", BooleanType(), True),
        StructField("controller_support", StringType(), True),
        StructField("header_image", StringType(), True),
        StructField("detailed_description", StringType(), True),
        StructField("about_the_game", StringType(), True),
        StructField("short_description", StringType(), True),
        StructField("supported_languages", StringType(), True),
        StructField("website", StringType(), True),
        StructField("developers", ArrayType(StringType()), True),
        StructField("publishers", ArrayType(StringType()), True),
        StructField(
            "price_overview",
            StructType(
                [
                    StructField("initial", LongType(), True),
                    StructField("final", LongType(), True),
                    StructField("discount_percent", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "platforms",
            StructType(
                [
                    StructField("windows", BooleanType(), True),
                    StructField("mac", BooleanType(), True),
                    StructField("linux", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "categories",
            ArrayType(
                StructType(
                    [
                        StructField("id", LongType(), True),
                        StructField("description", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "genres",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("description", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "screenshots",
            ArrayType(
                StructType(
                    [
                        StructField("id", LongType(), True),
                        StructField("path_thumbnail", StringType(), True),
                        StructField("path_full", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "movies",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField(
            "recommendations",
            StructType([StructField("total", LongType(), True)]),
            True,
        ),
        StructField(
            "achievements",
            StructType(
                [
                    StructField("total", LongType(), True),
                    StructField(
                        "highlighted",
                        ArrayType(
                            StructType(
                                [
                                    StructField("name", StringType(), True),
                                    StructField("path", StringType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField(
            "release_date",
            StructType(
                [
                    StructField("coming_soon", BooleanType(), True),
                    StructField("date", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "support_info",
            StructType(
                [
                    StructField("url", StringType(), True),
                    StructField("email", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("background", StringType(), True),
        StructField(
            "ratings",
            MapType(
                StringType(),
                StructType(
                    [
                        StructField("rating", StringType(), True),
                        StructField("required_age", StringType(), True),
                        StructField("descriptors", StringType(), True),
                    ]
                ),
            ),
            True,
        ),
        StructField(
            "package_groups",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("title", StringType(), True),
                        StructField(
                            "subs",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("packageid", LongType(), True),
                                        StructField(
                                            "price_in_cents_with_discount",
                                            LongType(),
                                            True,
                                        ),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
        StructField("steamspy_positive", LongType(), True),
        StructField("steamspy_negative", LongType(), True),
        StructField("steamspy_userscore", LongType(), True),
        StructField("steamspy_average_forever", LongType(), True),
        StructField("steamspy_average_2weeks", LongType(), True),
        StructField("steamspy_median_forever", LongType(), True),
        StructField("steamspy_median_2weeks", LongType(), True),
        StructField("steamspy_ccu", LongType(), True),
        StructField("steamspy_owners", StringType(), True),
    ]
)


raw_stream = (
    spark.readStream.format("kafka")
    .option(
        "kafka.bootstrap.servers", "my-kafka-cluster-kafka-bootstrap.kafka.svc:9092"
    )
    .option("subscribe", "steam-games-raw")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 1000)
    .load()
)

df_str = raw_stream.select(F.col("value").cast("string").alias("json_payload"))
df_map = df_str.select(
    F.from_json(F.col("json_payload"), MapType(StringType(), StringType())).alias(
        "data_map"
    )
)
df_exploded = df_map.select(F.explode("data_map").alias("appid", "json_value"))
df_parsed = df_exploded.withColumn(
    "game", F.from_json(F.col("json_value"), full_game_schema)
)

df_cleaned = df_parsed.select(
    F.col("appid"),
    F.coalesce(F.col("game.steam_appid").cast("int"), F.lit(None)).alias("steam_appid"),
    F.coalesce(F.col("game.type"), F.lit("")).alias("type"),
    F.coalesce(F.col("game.name"), F.lit("")).alias("name"),
    F.coalesce(F.col("game.required_age").cast("int"), F.lit(None)).alias(
        "required_age"
    ),
    F.coalesce(F.col("game.is_free"), F.lit(False)).alias("is_free"),
    F.coalesce(F.col("game.header_image"), F.lit("")).alias("header_image"),
    F.coalesce(F.col("game.controller_support"), F.lit("")).alias("controller_support"),
    F.coalesce(array_clean_native("game.developers"), F.array()).alias("developers"),
    F.coalesce(array_clean_native("game.publishers"), F.array()).alias("publishers"),
    F.coalesce(clean_text_native("game.detailed_description"), F.lit("")).alias(
        "detailed_description"
    ),
    F.coalesce(clean_text_native("game.about_the_game"), F.lit("")).alias(
        "about_the_game"
    ),
    F.coalesce(clean_text_native("game.short_description"), F.lit("")).alias(
        "short_description"
    ),
    F.coalesce(clean_text_native("game.supported_languages"), F.lit("")).alias(
        "supported_languages"
    ),
    F.coalesce(clean_ratings_udf(F.col("game")), F.lit("")).alias(
        "regions_description"
    ),
    F.coalesce(
        F.expr(
            "filter(transform(coalesce(game.categories, array()), x -> trim(x.description)), x -> x is not null and x != '')"
        ),
        F.array(),
    ).alias("categories"),
    F.coalesce(
        F.expr(
            "filter(transform(coalesce(game.genres, array()), x -> trim(x.description)), x -> x is not null and x != '')"
        ),
        F.array(),
    ).alias("genres"),
    F.coalesce(F.col("game.achievements.total"), F.lit(None)).alias(
        "achievements_total"
    ),
    F.coalesce(
        F.expr(
            "transform(coalesce(game.achievements.highlighted, array()), x -> x.name)"
        ),
        F.array(),
    ).alias("achievements_highlight"),
    F.coalesce(F.col("game.platforms.windows"), F.lit(False)).alias("windows"),
    F.coalesce(F.col("game.platforms.mac"), F.lit(False)).alias("mac"),
    F.coalesce(F.col("game.platforms.linux"), F.lit(False)).alias("linux"),
    F.coalesce(
        F.round(F.col("game.price_overview.initial").cast("double") / 100, 2),
        F.lit(None),
    ).alias("initial_price"),
    F.coalesce(
        F.round(F.col("game.price_overview.final").cast("double") / 100, 2), F.lit(None)
    ).alias("final_price"),
    F.coalesce(F.col("game.price_overview.discount_percent"), F.lit(None)).alias(
        "discount_percent"
    ),
    F.coalesce(F.col("game.recommendations.total"), F.lit(None)).alias(
        "total_rec_counts"
    ),
    parse_date_native("game.release_date.date").alias("release_date"),
    F.coalesce(F.col("game.release_date.coming_soon"), F.lit(False)).alias(
        "coming_soon"
    ),
    F.size(F.coalesce(F.col("game.screenshots"), F.array())).alias("num_screenshots"),
    F.coalesce(
        F.expr(
            """
            filter(
                flatten(transform(coalesce(game.package_groups, array()), g -> transform(coalesce(g.subs, array()), s -> s.price_in_cents_with_discount))),
                x -> x is not null
            )
        """
        ),
        F.array(),
    ).alias("package_prices"),
    F.coalesce(
        F.expr(
            "filter(transform(coalesce(game.movies, array()), m -> trim(m.name)), x -> x is not null and x != '')"
        ),
        F.array(),
    ).alias("movies"),
    F.coalesce(F.col("game.steamspy_positive").cast("int"), F.lit(None)).alias(
        "positive_reviews"
    ),
    F.coalesce(F.col("game.steamspy_negative").cast("int"), F.lit(None)).alias(
        "negative_reviews"
    ),
    F.coalesce(F.col("game.steamspy_userscore").cast("int"), F.lit(None)).alias(
        "userscore"
    ),
    F.coalesce(F.col("game.steamspy_average_forever").cast("int"), F.lit(None)).alias(
        "avg_playtime_forever"
    ),
    F.coalesce(F.col("game.steamspy_average_2weeks").cast("int"), F.lit(None)).alias(
        "avg_playtime_2weeks"
    ),
    F.coalesce(F.col("game.steamspy_median_forever").cast("int"), F.lit(None)).alias(
        "median_playtime_forever"
    ),
    F.coalesce(F.col("game.steamspy_median_2weeks").cast("int"), F.lit(None)).alias(
        "median_playtime_2weeks"
    ),
    F.coalesce(F.col("game.steamspy_ccu").cast("int"), F.lit(None)).alias(
        "concurrent_use"
    ),
    F.coalesce(
        F.regexp_replace(F.col("game.steamspy_owners"), r"\s*\.\.\s*", "-"), F.lit("")
    ).alias("owners"),
)


query = (
    df_cleaned.writeStream.format("iceberg")
    .outputMode("append")
    .trigger(processingTime="1 minute")
    .option("checkpointLocation", checkpoint_path)
    .option("fanout-enabled", "true")
    .toTable("nessie.silver.steam_games_landing")
)

query.awaitTermination()
