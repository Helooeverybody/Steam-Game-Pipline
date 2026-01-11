from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

spark = (
    SparkSession.builder.appName("SilverReviewAudit")
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

df = spark.read.format("iceberg").load("nessie.silver.steam_reviews_landing")

print("Running Critical Quarantine Logic...")

duplicate_ids = (
    df.groupBy("recommendationid")
    .count()
    .filter("count > 1")
    .select("recommendationid")
)

cond_is_duplicate = col("recommendationid").isin(
    duplicate_ids.rdd.flatMap(lambda x: x).collect()
)
cond_missing_id = col("recommendationid").isNull()
cond_missing_app_id = col("app_id").isNull()
cond_missing_author = col("author_steamid").isNull()

quarantine_flag = (
    cond_is_duplicate | cond_missing_id | cond_missing_app_id | cond_missing_author
)

quarantine_df = df.filter(quarantine_flag)
valid_df = df.filter(~quarantine_flag)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.quarantine")
quarantine_table = "nessie.quarantine.steam_reviews_bad"

print(f"Quarantining {quarantine_df.count()} rows to {quarantine_table}...")
quarantine_df.write.format("iceberg").mode("append").saveAsTable(quarantine_table)

print(f"Running Deequ Suites on {valid_df.count()} clean rows...")

# Critical Checks
check_critical = (
    Check(spark, CheckLevel.Error, "Critical Compliance Suite")
    .isComplete("recommendationid")
    .isUnique("recommendationid")
    .isComplete("app_id")
    .isComplete("author_steamid")
)

# Warning Checks
check_warning = (
    Check(spark, CheckLevel.Warning, "Business Logic Warning Suite")
    .isComplete("review")
    .isNonNegative("author_num_games_owned")
    .isNonNegative("author_num_reviews")
    .isNonNegative("author_playtime_forever")
    .isNonNegative("author_playtime_last_two_weeks")
    .isNonNegative("author_playtime_at_review")
    .isNonNegative("votes_up")
    .isNonNegative("votes_funny")
    .isNonNegative("comment_count")
    .hasMin("weighted_vote_score", lambda x: x == 0.0)
    .hasMax("weighted_vote_score", lambda x: x <= 1.0)
    .satisfies(
        "author_playtime_forever >= author_playtime_at_review",
        "playtime_consistency_check",
    )
    .isContainedIn("language", ["english"])
)

check_result = (
    VerificationSuite(spark)
    .onData(valid_df)
    .addCheck(check_critical)
    .addCheck(check_warning)
    .run()
)

# Reporting

check_result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
check_result_df.show(truncate=False)

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.dq_reports")
dq_table_name = "nessie.dq_reports.silver_steam_reviews_audit"

print(f"Saving DQ metrics to: {dq_table_name}")
check_result_df.write.format("iceberg").mode("append").saveAsTable(dq_table_name)

if check_result.status == "Error":
    print("CRITICAL ALERT: Critical Suite failed even after quarantine logic!")
    exit(1)

# --- MERGE LOGIC ---
print("Validations passed. Merging clean data into Gold/Curated Silver Table...")

valid_df.createOrReplaceTempView("valid_review_updates")

# Merge into the existing curated table
# steam_reviews primary key is recommendationid
spark.sql("""
    MERGE INTO nessie.silver.steam_reviews t
    USING valid_review_updates s
    ON t.recommendationid = s.recommendationid
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("Merge complete.")

# --- CLEANUP LANDING ---
print("Cleaning up processed data from Landing table...")

spark.sql("""
    DELETE FROM nessie.silver.steam_reviews_landing 
    WHERE recommendationid IN (SELECT recommendationid FROM valid_review_updates)
""")

print("Landing table cleanup complete.")

spark.stop()
