from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MyK3sPySparkJob").getOrCreate()

    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    columns = ["name", "id"]

    df = spark.createDataFrame(data, columns)

    print(f"Created a DataFrame with {df.count()} rows.")
    df.show()

    spark.stop()
