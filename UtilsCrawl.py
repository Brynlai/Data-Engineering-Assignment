import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, udf, lower
from pyspark.sql.types import StringType

class WordProcessor:
    def __init__(self, spark_session):
        """
        Initialize WordProcessor with a Spark session.
        """
        self.spark = spark_session

    @staticmethod
    def clean_word(word):
        """
        Cleans a word by converting it to lowercase and keeping only alphabetic characters.
        """
        return ''.join(char for char in word.lower() if char.isalpha())

    def process_words(self, combined_words_df, redis_client=None):
        """
        Cleans, deduplicates, and counts word frequencies from a Spark DataFrame.

        Args:
            combined_words_df: The input DataFrame containing a 'Word' column.
            redis_client: Optional Redis client for storing word frequencies.
                          If None, frequencies won't be saved to Redis.

        Returns:
            A Spark DataFrame containing distinct cleaned words.
        """
        # UDF for word cleaning
        clean_word_udf = udf(self.clean_word, StringType())

        # Split, explode, and clean words
        cleaned_words_df = (
            combined_words_df
            .withColumn("Word", explode(split(lower(combined_words_df["Word"]), "[,;]")))
            .withColumn("Cleaned_Word", clean_word_udf("Word"))
            .filter("Cleaned_Word != ''")
        )

        if redis_client:
            # Count and save word frequencies to Redis BEFORE deduplication
            word_frequencies_df = (
                cleaned_words_df.groupBy("Cleaned_Word")
                .count()
                .withColumnRenamed("count", "Frequency")
            )
            self.save_word_frequencies_to_redis(redis_client, word_frequencies_df)

        distinct_cleaned_words_df = cleaned_words_df.select("Cleaned_Word").distinct()

        print("Distinct Cleaned Combined Words:")
        distinct_cleaned_words_df.show(50, truncate=True)
        print("Distinct Cleaned Combined Words Count:", distinct_cleaned_words_df.count())

        return distinct_cleaned_words_df

    @staticmethod
    def save_word_frequencies_to_redis(redis_client, word_frequencies_df):
        """
        Saves word frequencies to Redis.
        """
        try:
            for row in word_frequencies_df.collect():
                redis_client.set(row["Cleaned_Word"], row["Frequency"])
        except Exception as e:
            print(f"Error saving to Redis: {e}")


class SparkManager:
    def __init__(self, app_name):
        """
        Initializes SparkManager with a SparkSession.
        """
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def load_csv(self, input_path, column_name):
        """
        Loads a CSV file and explodes the specified column into words.

        Args:
            input_path: Path to the CSV file.
            column_name: The column containing text data to split into words.

        Returns:
            A Spark DataFrame with words exploded into a 'Word' column.
        """
        article_csv_words = (
            self.spark.read.format("csv")
            .option("header", "true")
            .load(input_path)
        )

        print("Schema of the loaded DataFrame:")
        article_csv_words.printSchema()

        combined_words_df = article_csv_words.select(
            explode(split(column_name, "\\s+")).alias("Word")
        )

        print("Combined Words:")
        combined_words_df.show(10, truncate=True)
        print("Combined Words Count:", combined_words_df.count())

        return combined_words_df

    def stop(self):
        """
        Stops the Spark session.
        """
        self.spark.stop()
