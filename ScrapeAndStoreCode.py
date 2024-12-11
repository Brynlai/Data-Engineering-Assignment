from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests
from bs4 import BeautifulSoup
from typing import List

from Classes import Scraped_Data, Comment
from Definitions import fetch_definition
from Scrapes import scrape_comments, scrape_article

!pwd

# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()

# Scraping and data processing
base_url = "https://b.cari.com.my/portal.php?mod=view&aid="
aid_values = range(4, 5)  # Adjust range as needed

article_data = []
comments_data = []

for aid in aid_values:
    url = f"{base_url}{aid}"
    print(f"Scraping AID: {aid}")
    scraped_data = scrape_article(url, aid)
    if scraped_data:
        article = scraped_data[:-1]  # Exclude comments
        comments = scraped_data[-1]  # Extract comments
        article_data.append(article)
        comments_data.extend(comments)

# Defining schemas
article_schema = StructType([
    StructField("AID", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Publisher", StringType(), True),
    StructField("Views", IntegerType(), True),
    StructField("Comments_Count", IntegerType(), True),
    StructField("Content", StringType(), True),
])

comments_schema = StructType([
    StructField("AID", IntegerType(), True),
    StructField("Comment_ID", IntegerType(), True),
    StructField("User", StringType(), True),
    StructField("Comment_Text", StringType(), True),
])

# Creating DataFrames
article_df = spark.createDataFrame(article_data, schema=article_schema)
comments_df = spark.createDataFrame(comments_data, schema=comments_schema)

# Displaying data
print("Articles DataFrame:")
article_df.show(10, truncate=True)

print("Comments DataFrame:")
comments_df.show(10, truncate=True)

article_df.write.format("csv").mode("overwrite").option("header", "true").save("assignData/articles_data_csv")
article_csv = spark.read.csv('assignData/articles_data_csv', header=True)
comments_df.write.format("csv").mode("overwrite").option("header", "true").save("assignData/comments_data_csv")
comments_csv = spark.read.csv('assignData/comments_data_csv', header=True)

article_csv.show(5)
comments_csv.show(5)

# Definition part:
from pyspark.sql.functions import udf, split, col, explode
from Definitions import fetch_definition

definition_udf = udf(fetch_definition, StringType())

article_csv = spark.read.csv('assignData/articles_data_csv', header=True)
comments_csv = spark.read.csv('assignData/comments_data_csv', header=True)

print("article_csv: ")
article_csv.show(5)
print("comments_csv: ")
comments_csv.show(5)

# Split the text into words
article_csv_words = article_csv.withColumn("Title_Words", split(col("Title"), " ")) \
                         .withColumn("Content_Words", split(col("Content"), " "))

comments_csv_words = comments_csv.withColumn("Comment_Text_Words", split(col("Comment_Text"), " "))
print("article_csv_words: ")
article_csv_words.select("Content_Words").show(5)
print("comments_csv_words: ")
comments_csv_words.show(5)

# Clean individual Words

# # Apply the UDF to fetch definitions for each word
# article_csv_definitions = article_csv_words.select("Title_Words", "Content_Words") \
#                                         .withColumn("Word", explode("Content_Words")) \
#                                         .withColumn("Definition", definition_udf("Word")) \
#                                         .drop("Content_Words", "Title_Words")

# comments_csv_definitions = comments_csv_words.select("Comment_Text_Words") \
#                                          .withColumn("Word", explode("Comment_Text_Words")) \
#                                          .withColumn("Definition", definition_udf("Word")) \
#                                          .drop("Comment_Text_Words")

# # Display the results
# article_csv_definitions.show(5)
# comments_csv_definitions.show(5)

# # Count the rows in each DataFrame
# article_count = article_csv_definitions.count()
# comments_count = comments_csv_definitions.count()

# print(f"Article Definitions Count: {article_count}")
# print(f"Comments Definitions Count: {comments_count}")

# # Combine the DataFrames
# combined_definitions = article_csv_definitions.union(comments_csv_definitions)
# combined_count = combined_definitions.count()
# print(f"Combined Definitions Count: {combined_count}")

# # Save the combined DataFrame to a CSV file
# combined_definitions.write.option("header", True).csv("assignData/word_definitions_csv")

# word_definitions_csv = spark.read.csv('assignData/word_definitions_csv', header=True)
# word_definitions_csv.show(5)

