from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, DateType
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
input_path = "data/steam_apps_dataset_raw.json"



df = spark.read.option("multiline", True).json(input_path)
df = df.select(explode(F.map_entries(F.col("root"))).alias("entry"))
df = (
    df.select(F.explode(F.map_entries(F.col("root"))).alias("e"))
      .select(F.col("e.key").alias("appid"), F.col("e.value").alias("game"))
)
#-------------------------------------------

def array_clean(col):
    return F.expr(f"filter(transform({col}, x -> trim(x)), x -> x != '')")

def text_clean(col):
    return clean_text_udf(F.col(col))

df_cleaned = df.select(
    F.col("appid"),
    F.col("game.steam_appid").cast("int").alias("steam_appid"),
    F.col("game.type").alias("type"),
    F.col("game.name").alias("name"),
    F.col("game.required_age").cast("int"),
    F.col("game.is_free"),
    F.col("game.header_image"),
    F.expr("transform(game.dlc, d -> d)").alias("dlc"),
    array_clean("game.developers").alias("developers"),
    array_clean("game.publishers").alias("publishers"),
    text_clean("game.detailed_description").alias("detailed_description"),
    text_clean("game.about_the_game").alias("about_the_game"),
    text_clean("game.short_description").alias("short_description"),
    text_clean("game.supported_languages").alias("supported_languages"),
    clean_ratings_udf("game.ratings").alias("regions_description"),
    F.expr("transform(game.categories, x -> trim(x.description))").alias("categories"),
    F.expr("transform(game.genres, x -> trim(x.description))").alias("genres"),
    F.col("game.achievements.total").alias("achievements_total"),
    F.expr("transform(game.achievements.highlighted, x -> x.name)").alias("achievements_highlight"),
    F.col("game.platforms.windows").alias("windows"),
    F.col("game.platforms.mac").alias("mac"),
    F.col("game.platforms.linux").alias("linux"),
    normalize_price_udf("game.price_overview.initial").alias("initial_price"),
    normalize_price_udf("game.price_overview.final").alias("final_price"),
    F.col("game.recommendations.total").alias("total_rec_counts"),
    parse_date_udf("game.release_date.date").alias("release_date"),
    F.col("game.release_date.coming_soon").alias("coming_soon"),
    text_clean("game.pc_requirements.minimum").alias("pc_req_min"),
    text_clean("game.pc_requirements.recommended").alias("pc_req_rec"),
    text_clean("game.mac_requirements.minimum").alias("mac_req_min"),
    text_clean("game.mac_requirements.recommended").alias("mac_req_rec"),
    text_clean("game.linux_requirements.minimum").alias("linux_req_min"),
    text_clean("game.linux_requirements.recommended").alias("linux_req_rec"),
    F.size("game.screenshots").alias("num_screenshots"),
    F.expr("""
        filter(
            flatten(transform(game.package_groups, g -> transform(g.subs, s -> s.price_in_cents_with_discount))),
            x -> x is not null
        )
    """).alias("package_prices"),
    array_clean("game.movies.name").alias("movies"),
    F.col("steamspy_positive").cast("int"),
    F.col("steamspy_negative").cast("int"),
    F.col("steamspy_userscore").cast("int"),
    F.col("steamspy_average_forever").cast("int"),
    F.col("steamspy_average_2weeks").cast("int"),
    F.col("steamspy_median_forever").cast("int"),
    F.col("steamspy_median_2weeks").cast("int"),
    F.col("steamspy_ccu").cast("int").alias("concurrent_use")
)

df_cleaned.write.mode("overwrite").option("header", True).csv("cleaned_top_100_games_info.csv")
