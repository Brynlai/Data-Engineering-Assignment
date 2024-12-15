from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql.functions import udf, split, col, concat, regexp_replace, explode
from typing import List, Optional
from ForumClasses import Scraped_Data, Comment
from ForumScraper import scrape_data_udf
from UtilsRedis import save_word_frequencies_to_redis

import redis

# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()


# Base URL and AID range
aid_values = list(range(1, 50))  # Adjust range as needed

# Registering UDF
scrape_data = udf(scrape_data_udf, StructType([
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

# Creating an AID DataFrame for parallel processing
aid_df = spark.createDataFrame([(aid,) for aid in aid_values], ["AID"])

# Applying the UDF to scrape data
scraped_df = aid_df.withColumn("ScrapedData", scrape_data("AID"))

# Extracting articles and comments
article_df = scraped_df.selectExpr("ScrapedData.Article AS Article").select(
    "Article.*"
)
comments_df = scraped_df.selectExpr("ScrapedData.Comments AS Comments").selectExpr(
    "explode(Comments) AS Comment"
).select(
    "Comment.*"
)

# Displaying data
print("Articles DataFrame:")
article_df.show(50, truncate=True)

print("Comments DataFrame:")
comments_df.show(50, truncate=True)

# Writing DataFrames to CSV files with proper handling of quoted fields
article_df.write.option("header", True) \
    .mode("overwrite") \
    .option("quoteAll", True) \
    .option("escape", "\"") \
    .csv("assignData/articles_data_csv")

comments_df.write.option("header", True) \
    .mode("overwrite") \
    .option("quoteAll", True) \
    .option("escape", "\"") \
    .csv("assignData/comments_data_csv")


article_csv = spark.read.csv('assignData/articles_data_csv', header=True)
comments_csv = spark.read.csv('assignData/comments_data_csv', header=True)
print("article_csv.count(): ", article_csv.count())
print("comments_csv.count(): ", comments_csv.count())

# Use non empty rows.
filtered_article_csv = article_csv.filter((col("views").cast("int").isNotNull()) & (col("views") > 0))

# Concatenate Title and Content columns and replace commas with spaces
article_csv_words = filtered_article_csv.withColumn("Combined_Text", concat(col("Title"), col("Content"))) \
                                       .withColumn("Combined_Text", regexp_replace(col("Combined_Text"), ", ", " ")) \
                                       .withColumn("Combined_Words", split(col("Combined_Text"), " "))

# Split Comment Text into words by replacing commas with spaces
comments_csv_words = comments_csv.withColumn("Comment_Text", regexp_replace(col("Comment_Text"), ", ", " ")) \
                                 .withColumn("Comment_Text_Words", split(col("Comment_Text"), " "))


print("Article Words  Count:", article_csv_words.count())
print("Article Words  :", article_csv_words.show(10, truncate=True))
print("Comment Words  Count:", comments_csv_words.count())
print("Comment Words  :", comments_csv_words.show(10, truncate=True))

combined_words_df = article_csv_words.select(explode("Combined_Words").alias("Word"))
# Show the first few rows to verify
print("Combined Words:", combined_words_df.show(10, truncate=True) )
print("Combined Words Count:", combined_words_df.count())

from UtilsCleaner import process_words, save_word_frequencies_to_redis
# Example usage (assuming you have spark_session and combined_words_df defined):
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True) # or None if you don't need Redis
distinct_words_df = process_words(spark, combined_words_df, redis_client)

# Write the DataFrame to CSV
distinct_words_df.write.option("header", True) \
    .mode("overwrite") \
    .option("quoteAll", True) \
    .option("escape", "\"") \
    .csv("assignData/clean_words_data_csv")
