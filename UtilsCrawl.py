import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, udf, lower
from pyspark.sql.types import StringType

def clean_word(word):
    return ''.join(char for char in word.lower() if char.isalpha())

def process_words(spark_session, combined_words_df, redis_client=None):
    """
    Cleans, deduplicates, and counts word frequencies from a Spark DataFrame.

    Args:
        spark_session: The SparkSession object.
        combined_words_df: The input DataFrame containing a 'Word' column.
        redis_client: Optional redis client for storing word frequencies.
             If None, frequencies won't be saved to Redis.

    Returns:
        A Spark DataFrame containing distinct cleaned words.
    """

    # Split, explode, and clean words using native Spark functions for better performance
    cleaned_words_df = (combined_words_df
                       .withColumn("Word", explode(split(lower(combined_words_df["Word"]), "[,;]")))
                       .withColumn("Cleaned_Word", udf(clean_word, StringType())("Word"))
                       .filter("Cleaned_Word != ''"))

    if redis_client:
        # Count and save word frequencies to Redis BEFORE deduplication
        word_frequencies_df = cleaned_words_df.groupBy("Cleaned_Word").count().withColumnRenamed("count", "Frequency")
        save_word_frequencies_to_redis(redis_client, word_frequencies_df)

    distinct_cleaned_words_df = cleaned_words_df.select("Cleaned_Word").distinct()

    print("Distinct Cleaned Combined Words:")
    distinct_cleaned_words_df.show(50, truncate=True)
    print("Distinct Cleaned Combined Words Count:", distinct_cleaned_words_df.count())

    return distinct_cleaned_words_df

def save_word_frequencies_to_redis(redis_client, word_frequencies_df):
    """Saves word frequencies to Redis."""
    try:
        for row in word_frequencies_df.collect():
            redis_client.set(row["Cleaned_Word"], row["Frequency"])
    except Exception as e:
        print(f"Error saving to Redis: {e}")

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CleanWordsDataProcessor") \
        .getOrCreate()

    # Load data from assignmentData/Data_Streaming
    input_path = "assignmentData/Data_Streaming"
    article_csv_words = spark.read.format("csv") \
        .option("header", "true") \
        .load(input_path)

    # Inspect the DataFrame schema to confirm available columns
    print("Schema of the loaded DataFrame:")
    article_csv_words.printSchema()

    # Use the correct column (e.g., Cochabamba0) and split into individual words
    combined_words_df = article_csv_words.select(explode(split("Cochabamba0", "\\s+")).alias("Word"))

    # Show the first few rows to verify
    print("Combined Words:")
    combined_words_df.show(10, truncate=True)
    print("Combined Words Count:", combined_words_df.count())

    # Initialize Redis client
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

    # Process words using UtilsCleaner
    distinct_words_df = process_words(spark, combined_words_df, redis_client)

    # Write the DataFrame to CSV
    output_path = "assignmentData/clean_words_data_csv"
    distinct_words_df.write.option("header", True) \
        .mode("overwrite") \
        .option("quoteAll", True) \
        .option("escape", "\"") \
        .csv(output_path)

    print(f"Processed data saved to {output_path}")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
