from pyspark.sql.functions import split, explode, udf, lower
from pyspark.sql.types import StringType
import redis

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



