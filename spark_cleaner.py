from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, explode, lower, col

def clean_and_process_stream(topic: str, kafka_servers: str, hdfs_output_path: str):
    """
    Reads data from Kafka, processes it using PySpark, and writes the cleaned output to HDFS.
    """
    print("Starting Spark Structured Streaming job...")
    spark = SparkSession.builder \
        .appName("WikipediaDataCleaner") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    # Read data from Kafka topic
    print(f"Consuming data from Kafka topic: '{topic}'")
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .load()

    # Extract and clean the text
    print("Cleaning and splitting text into words...")
    cleaned_df = kafka_df.selectExpr("CAST(value AS STRING) as raw_text") \
        .withColumn("cleaned_text", regexp_replace("raw_text", r"[^a-zA-Z\s]", "")) \
        .withColumn("cleaned_text", lower(col("cleaned_text")))

    words_df = cleaned_df.withColumn("words", split(col("cleaned_text"), r"\s+")) \
        .select(explode("words").alias("word")) \
        .filter(col("word") != "")

    # Write cleaned words to HDFS (single batch execution)
    print(f"Writing cleaned words to HDFS at: '{hdfs_output_path}'")
    words_df.write.mode("overwrite").format("text").save(hdfs_output_path)
    print("Processing complete. Cleaned words successfully saved to HDFS.")

    spark.stop()
