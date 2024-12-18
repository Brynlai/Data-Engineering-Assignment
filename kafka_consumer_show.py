from pyspark.sql.functions import from_json, col, regexp_replace, split, explode
from pyspark.sql.types import StructType, StructField, StringType
from GlobalSparkSession import global_spark_session

def kafka_consumer():
    # Define the schema for the JSON messages
    schema = StructType([
        StructField("Title", StringType(), True),
        StructField("Content", StringType(), True)
    ])

    # Create Spark session
    spark = global_spark_session()

    # Read data from Kafka
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

    # Print the filtered words
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
    
    query.awaitTermination()
    query_to_csv.awaitTermination()


if __name__ == "__main__":
    kafka_consumer()
