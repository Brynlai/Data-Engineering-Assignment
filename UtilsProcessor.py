from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, BooleanType
from pyspark.sql.functions import udf, split, col, concat, regexp_replace, explode, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from typing import List

# Initialize SparkSession
from GlobalSparkSession import global_spark_session

import redis
import google.generativeai as genai

class ScrapedDataProcessor:
    """
    Processes scraped data using PySpark and Redis.
    """
    def __init__(self):
        """
        Initializes SparkSession and Redis client.
        """
        self.spark = global_spark_session()

        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

    def setup_udf(self, scrape_data_udf):
        """
        Sets up a user-defined function (UDF) for scraping data.

        Args:
            scrape_data_udf: The UDF function.
        """
        self.scrape_data = udf(scrape_data_udf, StructType([
            StructField("Article", StructType([
                StructField("AID", IntegerType(), True),
                StructField("Title", StringType(), True),
                StructField("Date", StringType(), True),
                StructField("Publisher", StringType(), True),
                StructField("Views", IntegerType(), True),
                StructField("Comments_Count", IntegerType(), True),
                StructField("Content", StringType(), True),
            ]), True),
            StructField("Comments", ArrayType(StructType([
                StructField("AID", IntegerType(), True),
                StructField("Comment_ID", IntegerType(), True),
                StructField("User", StringType(), True),
                StructField("Comment_Text", StringType(), True),
            ])), True)
        ]))

    def process_articles(self, aid_values):
        """
        Processes articles based on provided AID values.

        Args:
            aid_values: List of AID values.

        Returns:
            Tuple: article DataFrame and comments DataFrame.
        """
        aid_df = self.spark.createDataFrame([(aid,) for aid in aid_values], ["AID"])
        scraped_df = aid_df.withColumn("ScrapedData", self.scrape_data("AID"))

        article_df = scraped_df.selectExpr("ScrapedData.Article AS Article").select("Article.*")
        comments_df = scraped_df.selectExpr("ScrapedData.Comments AS Comments").selectExpr("explode(Comments) AS Comment").select("Comment.*")

        return article_df, comments_df

    def save_dataframes(self, article_df, comments_df):
        """
        Saves article and comments DataFrames to CSV files.

        Args:
            article_df: Article DataFrame.
            comments_df: Comments DataFrame.
        """
        article_df.write.option("header", True) \
            .mode("overwrite") \
            .option("quoteAll", True) \
            .option("escape", "\"") \
            .csv("assignData/articles_data_csv_test")

        comments_df.write.option("header", True) \
            .mode("overwrite") \
            .option("quoteAll", True) \
            .option("escape", "\"") \
            .csv("assignData/comments_data_csv_test")

    def process_words(self, article_csv_path, comments_csv_path):
        """
        Processes words from article and comment CSV files.

        Args:
            article_csv_path: Path to article CSV file.
            comments_csv_path: Path to comments CSV file.

        Returns:
            DataFrame: DataFrame containing combined words.
        """
        article_csv = self.spark.read.csv(article_csv_path, header=True)
        comments_csv = self.spark.read.csv(comments_csv_path, header=True)

        filtered_article_csv = article_csv.filter((col("views").cast("int").isNotNull()) & (col("views") > 0))
        article_csv_words = filtered_article_csv.withColumn("Combined_Text", concat(col("Title"), col("Content"))) \
                                               .withColumn("Combined_Text", regexp_replace(col("Combined_Text"), ", ", " ")) \
                                               .withColumn("Combined_Words", split(col("Combined_Text"), " "))

        comments_csv_words = comments_csv.withColumn("Comment_Text", regexp_replace(col("Comment_Text"), ", ", " ")) \
                                         .withColumn("Comment_Text_Words", split(col("Comment_Text"), " "))

        combined_words_df = article_csv_words.select(explode("Combined_Words").alias("Word"))

        return combined_words_df

    def save_cleaned_words(self, combined_words_df, process_words_func):
        """
        Saves cleaned words to a CSV file.

        Args:
            combined_words_df: DataFrame containing combined words.
            process_words_func: Function to process words.
        """
        distinct_words_df = process_words_func(self.spark, combined_words_df, self.redis_client)
        distinct_words_df.write.option("header", True) \
            .mode("overwrite") \
            .option("quoteAll", True) \
            .option("escape", "\"") \
            .csv("assignData/clean_words_data_csv_test")


class WordDetailsProcessor:
    """
    Processes word details using Google Gemini API.
    """
    def __init__(self, gemini_api):
        """
        Initializes SparkSession and Gemini API key.

        Args:
            gemini_api: Gemini API key.
        """
        self.spark = global_spark_session()

        self.gemini_api = gemini_api

    def read_clean_words(self, path):
        """
        Reads cleaned words from a CSV file.

        Args:
            path: Path to the CSV file.

        Returns:
            DataFrame: DataFrame containing cleaned words.
        """
        return self.spark.read.csv(path, header=True)

    def add_row_number(self, df):
        """
        Adds a row number column to the DataFrame.

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame: DataFrame with row number column.
        """
        print("add_row_number running")
        return df.withColumn("row_number", monotonically_increasing_id())

    def batch_process(self, df, batch_size, get_word_details_func):
        """
        Processes word details in batches using Gemini API.

        Args:
            df: DataFrame containing cleaned words.
            batch_size: Batch size for processing.
            get_word_details_func: Function to get word details.

        Returns:
            List: List of CSV data rows.
        """
        total_rows = df.count()
        all_csv_data = []

        for i in range(0, total_rows, batch_size):
            batch_df = df.filter((col("row_number") > i) & (col("row_number") <= i + batch_size))
            batch_words = batch_df.select("Cleaned_Word").rdd.map(lambda row: row[0]).collect()
            batch_csv_data = get_word_details_func(batch_words, self.gemini_api)

            batch_rows = batch_csv_data.strip().split("\n")
            if batch_rows[0].startswith('"word"'):
                batch_rows = batch_rows[1:]

            all_csv_data.extend(batch_rows)

        return all_csv_data

    def parse_and_save(self, all_csv_data, output_path):
        """
        Parses CSV data and saves it to a CSV file.

        Args:
            all_csv_data: List of CSV data rows.
            output_path: Output file path.
        """
        parsed_data = [row.split(',') for row in all_csv_data if len(row.split(',')) == 6]

        schema = StructType([
            StructField("word", StringType(), True),
            StructField("definition", StringType(), True),
            StructField("antonym", StringType(), True),
            StructField("synonym", StringType(), True),
            StructField("tatabahasa", StringType(), True),
            StructField("sentiment", StringType(), True),
        ])

        all_csv_data_df = self.spark.createDataFrame(parsed_data, schema=schema)
        all_csv_data_df.write.option("header", True) \
                             .mode("overwrite") \
                             .csv(output_path)

    def filter_usable_words(self, path, output_path):
        """
        Filters usable words from word details CSV.

        Args:
            path: Input CSV file path.
            output_path: Output CSV file path.
        """
        word_details_csv = self.spark.read.csv(path, header=True)

        def is_usable(definition, antonym, synonym):
            """
            Checks if a word is usable based on its definition, antonym, and synonym.

            Args:
                definition (str): The definition of the word.
                antonym (str): The antonym of the word.
                synonym (str): The synonym of the word.

            Returns:
                bool: True if the word is usable (at least one of definition, antonym, or synonym
                      does not contain "tidak diketahui" and "nama"), False otherwise.
            """
            # Use a list to store the different conditions.
            columns_to_check = [definition, antonym, synonym]
            for column in columns_to_check:
                if (column is not None) and "tidak diketahui" not in column.lower() and "nama" not in column.lower():
                    return True  # return true if one is usable
            return False # If none is usable, then return false

        is_usable_udf = udf(is_usable, BooleanType())
        cleaned_data = word_details_csv.filter(is_usable_udf(col("definition"), col("antonym"), col("synonym")))
        cleaned_data.write.csv(output_path, header=True, mode="overwrite")
