import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ML Imports
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    CountVectorizer,
    VectorAssembler
)
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Constants
CHECKPOINT_BASE = "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000/checkpoints/ml_models"
WAREHOUSE_PATH = "hdfs://my-hadoop-hadoop-hdfs-nn.hadoop.svc.cluster.local:9000/iceberg_data"
NESSIE_URI = "http://nessie.nessie-ns.svc:19120/api/v1"

def get_spark_session(app_name="SteamPlaytimePrediction_Train"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE_PATH)
        .getOrCreate()
    )

def preprocess_data(df):
    """
    Cleans raw Iceberg data for ML ingestion.
    FIX: Handles 'genres' and 'categories' as existing Arrays, not Strings.
    """
    return df.select(
        F.col("appid"),
        # Target Variable
        F.coalesce(F.col("avg_playtime_forever"), F.lit(0)).alias("label"),
        
        # Numerical Features
        F.coalesce(F.col("initial_price"), F.lit(0.0)).alias("initial_price"),
        F.coalesce(F.col("achievements_total"), F.lit(0)).alias("achievements_total"),
        F.coalesce(F.col("num_screenshots"), F.lit(0)).alias("num_screenshots"),
        
        # Boolean Feature
        F.when(F.col("is_free") == True, 1).otherwise(0).alias("is_free"),
        
        # Array Features: Coalesce NULL arrays to Empty Arrays []
        F.coalesce(F.col("genres"), F.array().cast("array<string>")).alias("genres_arr"),
        F.coalesce(F.col("categories"), F.array().cast("array<string>")).alias("categories_arr")
    )

def build_pipeline():
    """
    Constructs the SparkML Pipeline.
    REMOVED: RegexTokenizer (Data is already an Array)
    """
    
    # 1. Vectorize Genres (Input is now directly the Array column)
    vectorizer_genres = CountVectorizer(
        inputCol="genres_arr", 
        outputCol="genres_vec", 
        vocabSize=50,
        minDF=1.0 # Minimum Document Frequency (ignore super rare tags)
    )

    # 2. Vectorize Categories
    vectorizer_cats = CountVectorizer(
        inputCol="categories_arr", 
        outputCol="cats_vec", 
        vocabSize=50,
        minDF=1.0
    )

    # 3. Assemble features
    assembler = VectorAssembler(
        inputCols=[
            "initial_price", 
            "achievements_total", 
            "num_screenshots", 
            "is_free", 
            "genres_vec", 
            "cats_vec"
        ],
        outputCol="features"
    )

    # 4. Random Forest Regressor
    rf = RandomForestRegressor(
        featuresCol="features", 
        labelCol="label",
        numTrees=50,
        maxDepth=10,
        seed=42
    )

    return Pipeline(stages=[
        vectorizer_genres, 
        vectorizer_cats, 
        assembler, 
        rf
    ])

def main():
    spark = get_spark_session()
    
    # 1. Load Data
    print("Loading data from Nessie Silver...")
    raw_df = spark.read.format("iceberg").load("nessie.silver.steam_games")
    
    # 2. Preprocess
    print("Preprocessing data...")
    cleaned_df = preprocess_data(raw_df)
    
    # Debug: Print schema to verify types
    cleaned_df.printSchema()

    # 3. Split Data
    train_data, test_data = cleaned_df.randomSplit([0.8, 0.2], seed=42)
    
    # 4. Build and Train
    print("Training Random Forest Model...")
    pipeline = build_pipeline()
    model = pipeline.fit(train_data)

    # 5. Evaluate
    print("Evaluating Model...")
    predictions = model.transform(test_data)
    
    evaluator = RegressionEvaluator(
        labelCol="label", 
        predictionCol="prediction", 
        metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE): {rmse}")
    
    # Show predictions
    predictions.select("appid", "label", "prediction").show(5)

    # 6. Save Model
    model_path = f"{CHECKPOINT_BASE}/playtime_predictor_rf"
    print(f"Saving model to {model_path}...")
    model.write().overwrite().save(model_path)

    spark.stop()

if __name__ == "__main__":
    main()