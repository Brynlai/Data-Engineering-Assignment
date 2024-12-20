from pyspark.sql.functions import split, explode, udf, lower
from pyspark.sql.types import StringType
from UtilsRedis import Redis_Utilities
import redis

def clean_word(word):
    """
    Cleans a word by converting it to lowercase and removing non-alphanumeric characters.

    Args:
        word (str): The word to clean.

    Returns:
        str: The cleaned word.
    """
    return ''.join(char for char in word.lower() if char.isalpha())

def process_words(spark_session, combined_words_df, redis_client=redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)):
    """
    Cleans, deduplicates, and counts word frequencies from a Spark DataFrame.

    Args:
        spark_session (SparkSession): The SparkSession object.
        combined_words_df (DataFrame): The input DataFrame containing a 'Word' column.
        redis_client (redis.StrictRedis): Optional redis client for storing word frequencies.
             If None, frequencies won't be saved to Redis.

    Returns:
        DataFrame: A Spark DataFrame containing distinct cleaned words.
    """

    # Split, explode, and clean words using native Spark functions for better performance
    cleaned_words_df = (combined_words_df
                       .withColumn("Word", explode(split(lower(combined_words_df["Word"]), "[,;]"))) # Split words by comma and semicolon, explode into separate rows, and convert to lowercase
                       .withColumn("Cleaned_Word", udf(clean_word, StringType())("Word")) # Apply the clean_word function to each word
                       .filter("Cleaned_Word != ''")) # Filter out empty strings

    redis = Redis_Utilities()
    if redis:
        # Count word frequencies (before deduplication)
        word_frequencies_df = cleaned_words_df.groupBy("Cleaned_Word").count().withColumnRenamed("count", "Frequency")

        # Increment the frequencies in Redis
        redis.update_word_frequencies(word_frequencies_df)  # Use method from Redis_Utilities

    distinct_cleaned_words_df = cleaned_words_df.select("Cleaned_Word").distinct() # Deduplicate cleaned words

    print("Distinct Cleaned Combined Words:")
    distinct_cleaned_words_df.show(50, truncate=True) # Display the first 50 distinct cleaned words
    print("Distinct Cleaned Combined Words Count:", distinct_cleaned_words_df.count()) # Print the total count of distinct cleaned words

    return distinct_cleaned_words_df

