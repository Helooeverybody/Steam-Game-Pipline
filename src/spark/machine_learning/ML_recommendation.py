import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ML Imports
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# Constants
CHECKPOINT_BASE = "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000/checkpoints/ml_models"
WAREHOUSE_PATH = "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000/iceberg_data"
NESSIE_URI = "http://nessie.nessie-ns.svc:19120/api/v1"

def get_spark_session(app_name="SteamSmartRecommender_Train"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE_PATH)
        # ALS often requires more memory for stack traces
        .config("spark.executor.extraJavaOptions", "-Xss4m") 
        .config("spark.driver.extraJavaOptions", "-Xss4m")
        .getOrCreate()
    )

def load_and_preprocess_data(spark):
    """
    Loads Reviews and Games, filters junk, and computes the 'Smart Rating'.
    """
    print("Loading data from Nessie...")
    reviews_df = spark.read.format("iceberg").load("nessie.silver.steam_reviews")
    games_df = spark.read.format("iceberg").load("nessie.silver.steam_games")

    # 1. Filter: Only keep valid 'games' (No DLC, soundtracks, or applications)
    #    We assume 'type' column exists in steam_games.
    valid_games = games_df.filter(F.col("type") == "game").select("appid")

    # 2. Join: Inner join automatically removes reviews for DLCs/Soundtracks
    clean_reviews = reviews_df.join(valid_games, reviews_df.app_id == valid_games.appid)

    # 3. Engineer the 'Smart Rating'
    #    Formula: log1p(playtime) * sentiment_multiplier * purchase_multiplier
    
    # A. Log Normalization (Handle the huge variance between 1 hour and 5000 hours)
    #    Using log1p (log(x+1)) to safely handle 0s and compress the scale.
    df_scored = clean_reviews.withColumn("base_score", F.log1p(F.col("author_playtime_forever")))

    # B. Sentiment Weight (Reviewer gave a Thumbs Up?)
    #    True = 1.2x (Bonus)
    #    False = 0.5x (Penalty - played but hated it)
    #    Null = 1.0 (Neutral - played but didn't vote)
    df_scored = df_scored.withColumn(
        "sentiment_weight",
        F.when(F.col("voted_up") == True, 1.2)
         .when(F.col("voted_up") == False, 0.5)
         .otherwise(1.0)
    )

    # C. Purchase Weight (Paid vs Free)
    #    Paid = 1.0
    #    Free = 0.8 (Slight penalty to reduce noise from F2P hoarding)
    df_scored = df_scored.withColumn(
        "purchase_weight",
        F.when(F.col("received_for_free") == True, 0.8).otherwise(1.0)
    )

    # D. Final Calculation
    final_df = df_scored.withColumn(
        "smart_rating",
        F.col("base_score") * F.col("sentiment_weight") * F.col("purchase_weight")
    ).select(
        F.col("author_steamid").cast("string"), # Ensure String for Indexer
        F.col("app_id").cast("integer"),        # Ensure Int for Indexer/ALS
        F.col("smart_rating")
    )
    
    return final_df

def build_pipeline():
    """
    Constructs the Recommendation Pipeline.
    """
    # 1. User Indexer (String ID -> Integer Index 0 to N)
    #    setHandleInvalid("skip") allows us to ignore new/weird users during training
    user_indexer = StringIndexer(
        inputCol="author_steamid", 
        outputCol="user_index", 
        handleInvalid="skip"
    )

    # 2. Item Indexer (App ID -> Integer Index 0 to N)
    #    We convert the Integer AppID to String first internally for indexing if needed, 
    #    but usually StringIndexer works on numerics too. 
    #    To be safe with types, we might cast app_id to string in preprocessing or here.
    #    Note: StringIndexer requires input to be String or Numeric.
    item_indexer = StringIndexer(
        inputCol="app_id", 
        outputCol="item_index", 
        handleInvalid="skip"
    )

    # 3. ALS Model
    als = ALS(
        userCol="user_index",
        itemCol="item_index",
        ratingCol="smart_rating",
        implicitPrefs=True,  # Crucial: This tells Spark "smart_rating" is Confidence, not Stars
        coldStartStrategy="drop",
        nonnegative=True,    # Matrix factors should be positive
        rank=10,             # Number of latent factors (Hidden features)
        maxIter=10,          # Training iterations
        regParam=0.1         # Regularization to prevent overfitting
    )

    return Pipeline(stages=[user_indexer, item_indexer, als])

def main():
    spark = get_spark_session()
    
    # 1. Prepare Data
    training_data = load_and_preprocess_data(spark)
    
    # Cache because we use it multiple times (Index fitting + ALS training)
    training_data.cache()
    
    print(f"Data Loaded. Total interactions: {training_data.count()}")

    # 2. Split (Optional for ALS, but good practice to check if it runs)
    #    For production training on all data, you might skip splitting.
    #    Here we use 100% for training to maximize coverage.
    
    # 3. Train
    print("Training Smart ALS Model...")
    pipeline = build_pipeline()
    model = pipeline.fit(training_data)
    
    # 4. Save the Pipeline Model
    #    This saves the ALS weights AND the Indexers (mapping ID -> Int)
    model_path = f"{CHECKPOINT_BASE}/smart_recommender_als"
    print(f"Saving model to {model_path}...")
    model.write().overwrite().save(model_path)

    # 5. Sanity Check: Generate Recommendations for 5 Users
    print("Generating sample recommendations...")
    
    # Get the ALS model stage (last stage in pipeline)
    als_model = model.stages[-1]
    
    # Generate Top 5 recs
    user_recs = als_model.recommendForAllUsers(5)
    
    # We need to map back the Indices to Real App IDs
    # Get the item indexer model (2nd stage)
    item_indexer_model = model.stages[1]
    
    converter = IndexToString(
        inputCol="item_index", 
        outputCol="app_id", 
        labels=item_indexer_model.labels
    )
    
    # Explode and Convert
    exploded = user_recs.withColumn("rec", F.explode("recommendations")) \
                        .select("user_index", "rec.item_index", "rec.rating")
    
    readable_recs = converter.transform(exploded)
    
    # Join with Game Names for display
    games_df = spark.read.format("iceberg").load("nessie.silver.steam_games").select("appid", "name")
    
    # Note: IndexToString usually outputs String. Cast app_id to match.
    display_df = readable_recs.withColumn("app_id", F.col("app_id").cast("integer")) \
                              .join(games_df, "app_id") \
                              .select("user_index", "name", "rating")
    
    display_df.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
