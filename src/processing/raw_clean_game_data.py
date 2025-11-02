from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, BooleanType, ArrayType, StructType, StructField, DateType
from bs4 import BeautifulSoup
import re
from datetime import datetime
from dateutil import parser
from pyspark.sql.functions import explode, map_keys, map_values

RATING_TO_AGE = {
    # ESRB
    "ec": 3, "e": 6, "e10+": 10, "t": 13, "m": 17, "ao": 18,
    # PEGI
    "3": 3, "7": 7, "12": 12, "16": 16, "18": 18,
    # USK
    "0": 0, "6": 6, "12": 12, "16": 16, "18": 18,
    # KGRB
    "all": 0, "12": 12, "15": 15, "18": 18,
    # AGCOM
    "3": 3, "7": 7, "12": 12, "16": 16, "18": 18,
    # CADPA
    "12": 12, "16": 16, "18": 18,
    # DEJUS
    "l": 0, "10": 10, "12": 12, "14": 14, "16": 16, "18": 18,
    # Steam Germany
    "0": 0, "6": 6, "12": 12, "16": 16, "18": 18,
    # OFLC
    "g": 0, "pg": 10, "m": 15, "ma15": 15, "r16": 16, "r18": 18,
    # NZOFLC
    "g": 0, "pg": 10, "r13": 13, "r15": 15, "r16": 16, "r18": 18,
    # CERO
    "a": 0, "b": 12, "c": 15, "d": 17, "z": 18,
    # GMEDIA / MDA
    "pg13": 13, "nc16": 16, "m18": 18,
    # FPB
    "10": 10, "13": 13, "16": 16, "18": 18,
    # CSRR
    "g": 0, "7": 7, "13": 13, "15": 15, "18": 18,
    # BBFC
    "u": 0, "pg": 10, "12": 12, "15": 15, "18": 18,
    # CRE
    "3": 3, "7": 7, "12": 12, "16": 16, "18": 18,
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
    keys = ["rating", "descriptors","required_age"]
    ratings = game.get("ratings", {})
    if not isinstance(ratings, dict) or not ratings:
        return ""
    
    formatted_regions = []
    for region, info in ratings.items():
        if not isinstance(info, dict):
            continue

        age = info.get("required_age")
        if age is None or str(age).strip() == "":
            rating_symbol = str(info.get("rating", "")).lower().strip()
            age = RATING_TO_AGE.get(rating_symbol, 0)  # default 0 if not recognized
        else:
            try:
                age = int(age)
            except ValueError:
                rating_symbol = str(age).lower().strip()
                age = RATING_TO_AGE.get(rating_symbol, 0)

        # --- Determine descriptors ---
        descriptors = info.get("descriptors", "")
        if isinstance(descriptors, list):
            descriptors = ", ".join(map(clean_text, descriptors))
        else:
            descriptors = clean_text(descriptors)

        # --- Build cleaned string for region ---
        region_str = f"{region}: (required_age={age}, descriptors={descriptors})"
        formatted_regions.append(region_str)

    return " | ".join(formatted_regions)     

def parse_date(date_str):
    if not date_str or not isinstance(date_str, str):
        return None
    month_map = {
        "janv.": "Jan", "févr.": "Feb", "mars": "Mar", "avr.": "Apr", "mai": "May",
        "juin": "Jun", "juil.": "Jul", "août": "Aug", "sept.": "Sep", "oct.": "Oct",
        "nov.": "Nov", "déc.": "Dec",
        "ene.": "Jan", "abr.": "Apr", "ago.": "Aug", "dic.": "Dec",
        "märz": "Mar", "okt.": "Oct", "dez.": "Dec"
    }
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



clean_text_udf = F.udf(clean_text, StringType())
spark = SparkSession.builder.appName("SteamGameCleaner").getOrCreate()

clean_ratings_udf = F.udf(clean_ratings, StringType())
input_path = "data/top_100_games_info.json"
df = spark.read.option("multiline", True).json(input_path)


df = spark.read.option("multiline", True).json(input_path)
df = df.select(explode(F.map_entries(F.col("root"))).alias("entry"))
df = df.select(F.col("entry.key").alias("appid"), F.col("entry.value").alias("game"))

parse_date_udf = F.udf(parse_date, DateType())
df_cleaned = (
    df
    # --- Basic metadata ---
    .withColumn("steam_appid", F.col("game.steam_appid").cast(IntegerType()))
    .withColumn("type", F.coalesce(F.col("game.type").cast(StringType()), F.lit("")))
    .withColumn("name", F.coalesce(F.col("game.name").cast(StringType()), F.lit("")))
    .withColumn("required_age", F.coalesce(F.col("game.required_age").cast(IntegerType()), F.lit(None)))
    .withColumn("is_free", F.coalesce(F.col("game.is_free").cast(BooleanType()), F.lit(None)))
    .withColumn("header_image", F.coalesce(F.col("game.header_image").cast(StringType()), F.lit("")))
    .withColumn("dlc", F.when(F.col("game.dlc").isNotNull(), F.expr("transform(game.dlc, d -> d)")).otherwise(F.array()))
    .withColumn("developers", F.when(F.col("game.developers").isNotNull(),
                                     F.expr("transform(game.developers, d -> trim(d))")).otherwise(F.array()))
    .withColumn("publishers", F.when(F.col("game.publishers").isNotNull(),
                                     F.expr("transform(game.publishers, p -> trim(p))")).otherwise(F.array()))

    # --- Text fields ---
    .withColumn("detailed_description", F.when(F.col("game.detailed_description").isNotNull(),
                                               clean_text_udf(F.col("game.detailed_description"))).otherwise(F.lit("")))
    .withColumn("about_the_game", F.when(F.col("game.about_the_game").isNotNull(),
                                         clean_text_udf(F.col("game.about_the_game"))).otherwise(F.lit("")))
    .withColumn("short_description", F.when(F.col("game.short_description").isNotNull(),
                                            clean_text_udf(F.col("game.short_description"))).otherwise(F.lit("")))
    .withColumn("supported_languages", F.when(F.col("game.supported_languages").isNotNull(),
                                              clean_text_udf(F.col("game.supported_languages"))).otherwise(F.lit("")))


    .withColumn("regions_description", F.when(F.col("game.ratings").isNotNull(),
                                              clean_ratings_udf(F.col("game.ratings"))).otherwise(F.lit("")))

    # --- Categories & Genres ---
    .withColumn("categories", F.when(F.col("game.categories").isNotNull(),
                                     F.expr("transform(game.categories, c -> trim(c.description))")).otherwise(F.array()))
    .withColumn("genres", F.when(F.col("game.genres").isNotNull(),
                                 F.expr("transform(game.genres, g -> trim(g.description))")).otherwise(F.array()))

    # --- Achievements ---
    .withColumn("achievements_total", F.coalesce(F.col("game.achievements.total"), F.lit(0)))
    .withColumn("achievements_highlight",
                F.when(F.col("game.achievements.highlighted").isNotNull(),
                       F.expr("""
                           filter(
                               transform(game.achievements.highlighted, h -> h.name),
                               x -> x is not null and x != ''
                           )
                       """))
                .otherwise(F.array()))

    # --- Platforms ---
    .withColumn("windows", F.coalesce(F.col("game.platforms.windows"), F.lit(False)))
    .withColumn("mac", F.coalesce(F.col("game.platforms.mac"), F.lit(False)))
    .withColumn("linux", F.coalesce(F.col("game.platforms.linux"), F.lit(False)))

    # --- Prices and Recommendations ---
    .withColumn("final_price", F.coalesce(F.col("game.price_overview.final_formatted"), F.lit("")))
    .withColumn("total_rec_counts", F.coalesce(F.col("game.recommendations.total"), F.lit(0)))

    # --- Release Date ---
    .withColumn("release_date", F.when(F.col("game.release_date.date").isNotNull(),
                                       parse_date_udf(F.col("game.release_date.date"))).otherwise(F.lit(None)))
    .withColumn("coming_soon", F.coalesce(F.col("game.release_date.coming_soon"), F.lit(False)))

    # --- Requirements ---
    .withColumn("pc_req_min", F.when(F.col("game.pc_requirements").isNotNull(),
                                     clean_text_udf(F.col("game.pc_requirements.minimum"))).otherwise(F.lit("")))
    .withColumn("pc_req_rec", F.when(F.col("game.pc_requirements").isNotNull(),
                                     clean_text_udf(F.col("game.pc_requirements.recommended"))).otherwise(F.lit("")))
    .withColumn("mac_req_min", F.when(F.col("game.mac_requirements").isNotNull(),
                                      clean_text_udf(F.col("game.mac_requirements.minimum"))).otherwise(F.lit("")))
    .withColumn("mac_req_rec", F.when(F.col("game.mac_requirements").isNotNull(),
                                      clean_text_udf(F.col("game.mac_requirements.recommended"))).otherwise(F.lit("")))
    .withColumn("linux_req_min", F.when(F.col("game.linux_requirements").isNotNull(),
                                        clean_text_udf(F.col("game.linux_requirements.minimum"))).otherwise(F.lit("")))
    .withColumn("linux_req_rec", F.when(F.col("game.linux_requirements").isNotNull(),
                                        clean_text_udf(F.col("game.linux_requirements.recommended"))).otherwise(F.lit("")))

    # --- Screenshots ---
    .withColumn("num_screenshots",
                F.when(F.col("game.screenshots").isNotNull(),
                       F.size(F.col("game.screenshots"))).otherwise(F.lit(0)))

    # --- Package Prices ---
    .withColumn("package_prices",
                F.expr("""
                    filter(
                        flatten(
                            transform(
                                game.package_groups,
                                g -> transform(g.subs, s -> s.price_in_cents_with_discount)
                            )
                        ),
                        x -> x is not null
                    )
                """))

    # --- Movies ---
    .withColumn("movies",
                F.when(F.col("game.movies").isNotNull(),
                       F.expr("""
                           filter(
                               transform(game.movies, m -> trim(m.name)),
                               x -> x is not null and x != ''
                           )
                       """))
                .otherwise(F.array()))
)

df_cleaned.write.mode("overwrite").option("header", True).csv("cleaned_top_100_games_info.csv")
