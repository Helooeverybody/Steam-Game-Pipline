from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, window, lit
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

spark = (
    SparkSession.builder.appName("SilverDataAudit")
    .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.7-spark-3.3")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    .config(
        "spark.sql.catalog.nessie.uri",
        "http://nessie.nessie-ns.svc.cluster.local:19120/api/v1",
    )
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.authentication.type", "NONE")
    .config(
        "spark.sql.catalog.nessie.warehouse",
        "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000/iceberg_data",
    )
    .getOrCreate()
)

df = spark.read.format("iceberg").load("nessie.silver.steam_games_landing")

print("Running Critical Quarantine Logic...")

# We calculate counts per appid. If > 1, ALL instances are flagged for review.
duplicate_ids = df.groupBy("appid").count().filter("count > 1").select("appid")

# These mirror the 'Critical Suite' rules. If a row fails these, it gets quarantined.
cond_is_duplicate = col("appid").isin(duplicate_ids.rdd.flatMap(lambda x: x).collect())
cond_missing_id = col("appid").isNull()
cond_missing_name = col("name").isNull()
cond_invalid_type = (col("type").isNull()) | (col("type") != "game")
cond_id_mismatch = col("appid") != col("steam_appid")

quarantine_flag = (
    cond_is_duplicate
    | cond_missing_id
    | cond_missing_name
    | cond_invalid_type
    | cond_id_mismatch
)

quarantine_df = df.filter(quarantine_flag)
valid_df = df.filter(~quarantine_flag)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.quarantine")
quarantine_table = "nessie.quarantine.steam_games_bad"

print(f"Quarantining {quarantine_df.count()} rows to {quarantine_table}...")
quarantine_df.write.format("iceberg").mode("append").saveAsTable(quarantine_table)

print(f"Running Deequ Suites on {valid_df.count()} clean rows...")

# Critical Checks (Must pass to be merged)
check_critical = (
    Check(spark, CheckLevel.Error, "Critical Compliance Suite")
    .isComplete("appid")
    .isUnique("appid")
    .satisfies("appid = steam_appid", "appid_match_steam_appid")
    .isComplete("name")
    .isComplete("type")
    .isContainedIn("type", ["game"])
)

# Warning Checks (Just for reporting, does not block merge)
check_warning = (
    Check(spark, CheckLevel.Warning, "Business Logic Warning Suite")
    .isNonNegative("initial_price")
    .isNonNegative("final_price")
    .hasMin("discount_percent", lambda x: x == 0)
    .hasMax("discount_percent", lambda x: x <= 100)
    .isNonNegative("positive_reviews")
    .isNonNegative("negative_reviews")
    .isNonNegative("required_age")
    .hasMax("required_age", lambda x: x <= 130)
    .satisfies(
        "(is_free = true AND final_price = 0.0) OR (is_free = false)",
        "free_games_have_zero_price",
    )
    .hasPattern("owners", r"^[\d,]+-[\d,]+$")
    .hasPattern(
        "header_image",
        r"^https?://shared\.akamai\.steamstatic\.com/store_item_assets/steam/apps/*",
    )
    .satisfies(
        "windows = true OR mac = true OR linux = true",
        "at_least_one_platform_supported",
    )
    .isNonNegative("achievements_total")
)

# Run Verification
check_result = (
    VerificationSuite(spark)
    .onData(valid_df)
    .addCheck(check_critical)
    .addCheck(check_warning)
    .run()
)

check_result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
check_result_df.show(truncate=False)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.dq_reports")
dq_table_name = "nessie.dq_reports.silver_steam_games_audit"

print(f"Saving DQ metrics to: {dq_table_name}")
check_result_df.write.format("iceberg").mode("append").saveAsTable(dq_table_name)

if check_result.status == "Error":
    print("CRITICAL ALERT: Critical Suite failed! Aborting merge for this batch.")
    # In a real system, you might want to alert here.
    # We exit to avoid merging potentially bad data that slipped through the manual filters.
    exit(1)

# --- MERGE LOGIC ---
print("Validations passed. Merging clean data into Gold/Curated Silver Table...")

# Create a temporary view for the valid data to use in SQL
valid_df.createOrReplaceTempView("valid_updates")

# Merge into the existing curated table
# We match on appid. If matched, we update. If not, we insert.
spark.sql("""
    MERGE INTO nessie.silver.steam_games t
    USING valid_updates s
    ON t.appid = s.appid
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("Merge complete.")

# --- CLEANUP LANDING ---
# IMPORTANT: In a production 'landing' pattern, you usually want to delete the data you just processed
# so the next run doesn't re-process it.
print("Cleaning up processed data from Landing table...")

# We delete all rows from landing that match the appids we just processed (or simply truncate if we assume strict batching)
# Using a subquery delete for safety:
spark.sql("""
    DELETE FROM nessie.silver.steam_games_landing 
    WHERE appid IN (SELECT appid FROM valid_updates)
""")

print("Landing table cleanup complete.")

spark.stop()
