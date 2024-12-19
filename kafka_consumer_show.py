from pyspark.sql.functions import from_json, col, regexp_replace, split, explode
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

def kafka_consumer():
    """
    Consumes data from Kafka, processes it, and saves it to a CSV file.

    This function reads data from a Kafka topic, extracts title and content from JSON messages,
    cleans the content, splits it into words, filters out empty words, and saves the results
    to a CSV file. It also prints the filtered words to the console.
    """
    # Define the schema for the JSON messages
    schema = StructType([
        StructField("Title", StringType(), True),
        StructField("Content", StringType(), True)
    ])

    # Create Spark session
    spark = SparkSession.builder \
        .appName("data-engineering-project") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

    # Read data from Kafka
    # Spark Structured Streaming starts here
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "wiki_topic") \
        .load()

    # Convert the value from binary to string
    json_df = df.selectExpr("CAST(value AS STRING) as json")

    # Extract Title and Content from JSON
    messages_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

    # Clean the content by removing non-alphabetic characters
    cleaned_df = messages_df.withColumn("Cleaned_Content", regexp_replace(col("Content"), "[^a-zA-Z ]", ""))

    # Split the cleaned content into individual words
    split_words_df = cleaned_df.withColumn("Words", split(col("Cleaned_Content"), " "))

    # Explode the list of words into individual rows
    exploded_words_df = split_words_df.select(explode(col("Words")).alias("Word"))

    # Filter out empty words
    filtered_words_df = exploded_words_df.filter(col("Word") != "")

    # Print the filtered words to the console
    query = filtered_words_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Save the filtered words to a CSV file
    query_to_csv = filtered_words_df.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("header", "true") \
        .option("path", "assignData/wiki_word_data_csv_test") \
        .option("checkpointLocation", "assignData/wiki_word_data_checkpoint_test") \
        .start()
    
    # Wait for both queries to terminate
    query.awaitTermination()
    query_to_csv.awaitTermination()


if __name__ == "__main__":
    kafka_consumer()
