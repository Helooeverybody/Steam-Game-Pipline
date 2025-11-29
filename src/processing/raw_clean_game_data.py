from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, DateType, MapType
from bs4 import BeautifulSoup
import re
from datetime import datetime
from dateutil import parser
from pyspark.sql.functions import explode, map_keys, map_values

RATING_TO_AGE = {
    "ec": 3, "e": 6, "e10+": 10, "t": 13, "m": 17, "ao": 18,
    "3": 3, "7": 7, "12": 12, "16": 16, "18": 18,
    "all": 0, "l": 0, "g": 0, "u": 0, "a": 0,
    "pg": 10, "r13": 13, "r15": 15, "ma15": 15, "nc16": 16,
    "r16": 16, "r18": 18, "m18": 18, "b": 12, "c": 15, "d": 17, "z": 18,
}
month_map = {
    "janv.": "Jan", "févr.": "Feb", "mars": "Mar", "avr.": "Apr", "mai": "May",
    "juin": "Jun", "juil.": "Jul", "août": "Aug", "sept.": "Sep", "oct.": "Oct",
    "nov.": "Nov", "déc.": "Dec",
    "ene.": "Jan", "abr.": "Apr", "ago.": "Aug", "dic.": "Dec",
    "märz": "Mar", "okt.": "Oct", "dez.": "Dec"
}

def clean_text(html_text):
    if not html_text or not isinstance(html_text, str):
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    for br in soup.find_all(["br", "li"]):
        br.insert_before("; ")
    text = soup.get_text(separator=" ")
    text = (text
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("®", "")
        .replace("™", "")
        .replace("©", "(c)")
        .replace("–", "-")
        .replace("—", "-"))
    text = re.sub(r'\s*[\n\r]+', ' ', text)
    text = text.replace('\xa0', ' ')
    text = re.sub(r'\s*-\s*', ' - ', text)
    text = re.sub(r'\s*([.,;:!?])\s*', r'\1 ', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\*+', '', text)
    text = re.sub(r';\s*;', ';', text)
    text = re.sub(r'\s*;\s*', '; ', text)
    text = re.sub(r'(?<=\w:)\s+', ' ', text)
    text = re.sub(r'(?<=\w\.)\s*(?=[A-Z])', ' ', text)
    return text.strip(" ;")

def clean_ratings(game):
    if game is None:
        return ""
    # Get ratings safely
    ratings = getattr(game, "ratings", {})  
    if ratings is None:
        return ""

    # Convert Row to dict if needed
    if isinstance(ratings, list):  # sometimes nested lists
        ratings = dict(ratings)
    elif isinstance(ratings, Row):
        ratings = ratings.asDict()

    formatted_regions = []

    for region, info in (ratings.items() if isinstance(ratings, dict) else []):
        if info is None:
            continue
        # age
        age = getattr(info, "required_age", None) if isinstance(info, Row) else info.get("required_age", None)
        if age is None or str(age).strip() == "":
            rating_symbol = (getattr(info, "rating", "") if isinstance(info, Row) else info.get("rating", "")).lower().strip()
            age = RATING_TO_AGE.get(rating_symbol, 0)
        else:
            try:
                age = int(age)
            except ValueError:
                rating_symbol = str(age).lower().strip()
                age = RATING_TO_AGE.get(rating_symbol, 0)

        # descriptors
        descriptors = getattr(info, "descriptors", "") if isinstance(info, Row) else info.get("descriptors", "")
        if isinstance(descriptors, list):
            descriptors = ", ".join(map(clean_text, descriptors))
        else:
            descriptors = clean_text(descriptors)

        region_str = f"{region}: (required_age={age}, descriptors={descriptors})"
        formatted_regions.append(region_str)

    return " | ".join(formatted_regions)
 


def parse_date(date_str):
    if not date_str or not isinstance(date_str, str):
        return None
    for local, eng in month_map.items():
        if local.lower() in date_str.lower():
            date_str = re.sub(local, eng, date_str, flags=re.IGNORECASE)
            break
    for fmt in ("%d %b, %Y", "%b %d, %Y", "%d %b %Y", "%b %Y", "%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    try:
        return parser.parse(date_str, fuzzy=True).date()
    except Exception:
        return None
def normalize_price(p):
    try:
        return round(float(p) / 100.0, 2)
    except:
        return None

clean_text_udf = F.udf(clean_text, StringType())
parse_date_udf = F.udf(parse_date, DateType())
clean_ratings_udf = F.udf(clean_ratings, StringType())
normalize_price_udf = F.udf(normalize_price, DoubleType())


#------------------------------------

spark = SparkSession.builder.appName("SteamGameCleaner").getOrCreate()
input_path = "s3a://spark-scripts/steam_apps_dataset_first_ids.json"

df = spark.read.option("multiline", True).json(input_path)
first_col = df.columns[0]
game_schema = df.schema[first_col].dataType

# 3. Convert dynamic columns → Map(AppID → GameStruct)
df = df.select(
    F.from_json(
        F.to_json(F.struct(*df.columns)),
        MapType(StringType(), game_schema)
    ).alias("data_map")
)

# 4. Explode into rows
df = df.select(F.explode("data_map").alias("appid", "game"))
#-------------------------------------------

def array_clean(col):
    return F.expr(f"filter(transform({col}, x -> trim(x)), x -> x != '')")

def text_clean(col):
    return clean_text_udf(F.col(col))

df_cleaned = df.select(
  F.col("appid"),
    # --- Basic fields ---
    F.coalesce(F.col("game.steam_appid").cast("int"), F.lit(None)).alias("steam_appid"),
    F.coalesce(F.col("game.type"), F.lit("")).alias("type"),
    F.coalesce(F.col("game.name"), F.lit("")).alias("name"),
    F.coalesce(F.col("game.required_age").cast("int"), F.lit(None)).alias("required_age"),
    F.col("game.is_free"),
    F.coalesce(F.col("game.header_image"), F.lit("")).alias("header_image"),

    # --- Arrays (safe defaults []) ---
    F.coalesce(array_clean("game.developers"), F.array()).alias("developers"),
    F.coalesce(array_clean("game.publishers"), F.array()).alias("publishers"),

    # --- Text fields ---
    F.coalesce(text_clean("game.detailed_description"), F.lit("")).alias("detailed_description"),
    F.coalesce(text_clean("game.about_the_game"), F.lit("")).alias("about_the_game"),
    F.coalesce(text_clean("game.short_description"), F.lit("")).alias("short_description"),
    F.coalesce(text_clean("game.supported_languages"), F.lit("")).alias("supported_languages"),

    # ratings
    F.coalesce(clean_ratings_udf("game.ratings"), F.lit("")).alias("regions_description"),

    # categories/genres
    F.coalesce(F.expr("transform(game.categories, x -> trim(x.description))"), F.array()).alias("categories"),
    F.coalesce(F.expr("transform(game.genres, x -> trim(x.description))"), F.array()).alias("genres"),

    # achievements
    F.coalesce(F.col("game.achievements.total"), F.lit(None)).alias("achievements_total"),
    F.coalesce(F.expr("transform(coalesce(game.achievements.highlighted, array()), x -> x.name)"), F.array()).alias("achievements_highlight"),

    # platforms
    F.coalesce(F.col("game.platforms.windows"), F.lit(False)).alias("windows"),
    F.coalesce(F.col("game.platforms.mac"), F.lit(False)).alias("mac"),
    F.coalesce(F.col("game.platforms.linux"), F.lit(False)).alias("linux"),

    # price overview
    F.coalesce(normalize_price_udf("game.price_overview.initial"), F.lit(None)).alias("initial_price"),
    F.coalesce(normalize_price_udf("game.price_overview.final"), F.lit(None)).alias("final_price"),
    F.coalesce(F.col("game.price_overview.discount_percent"), F.lit(None)).alias("discount_percent"),

    # recommendations
    F.coalesce(F.col("game.recommendations.total"), F.lit(None)).alias("total_rec_counts"),

    # release date
    F.coalesce(parse_date_udf("game.release_date.date"), F.lit(None)).alias("release_date"),
    F.coalesce(F.col("game.release_date.coming_soon"), F.lit(False)).alias("coming_soon"),

    # screenshots
    F.coalesce(F.size("game.screenshots"), F.lit(0)).alias("num_screenshots"),

    # package prices
    F.coalesce(
          F.expr("""
            filter(
                flatten(transform(coalesce(game.package_groups, array()), g -> transform(coalesce(g.subs, array()), s -> s.price_in_cents_with_discount))),
                x -> x is not null
            )
        """),
        F.array()
    ).alias("package_prices"),

    # movies
    F.coalesce(array_clean("game.movies.name"), F.array()).alias("movies"),

    # steamspy stats
    F.coalesce(F.col("game.steamspy_positive").cast("int"), F.lit(None)).alias("positive_reviews"),
    F.coalesce(F.col("game.steamspy_negative").cast("int"), F.lit(None)).alias("negative_reviews"),
    F.coalesce(F.col("game.steamspy_userscore").cast("int"), F.lit(None)).alias("userscore"),

    F.coalesce(F.col("game.steamspy_average_forever").cast("int"), F.lit(None)).alias("avg_playtime_forever"),
    F.coalesce(F.col("game.steamspy_average_2weeks").cast("int"), F.lit(None)).alias("avg_playtime_2weeks"),
    F.coalesce(F.col("game.steamspy_median_forever").cast("int"), F.lit(None)).alias("median_playtime_forever"),
    F.coalesce(F.col("game.steamspy_median_2weeks").cast("int"), F.lit(None)).alias("median_playtime_2weeks"),
    F.coalesce(F.col("game.steamspy_ccu").cast("int"), F.lit(None)).alias("concurrent_use"),

    # owners cleanup
    F.coalesce(F.regexp_replace(F.col("game.steamspy_owners"), r"\s*\.\.\s*", "-"), F.lit("")).alias("owners")
)

output_path = "s3a://spark-scripts/steam_apps_cleaned.parquet"
df_cleaned.write.mode("overwrite").parquet(output_path)