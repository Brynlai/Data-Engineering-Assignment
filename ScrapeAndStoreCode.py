from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests
from bs4 import BeautifulSoup
from typing import List

from Classes import Scraped_Data, Comment
from Definitions import fetch_definition
from Scrapes import scrape_comments, scrape_article
# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()

# Scraping and data processing
base_url = "https://b.cari.com.my/portal.php?mod=view&aid="
aid_values = range(1, 501)  # Adjust range as needed

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

article_df.write.format("csv").mode("overwrite").option("header", "true").save("assignmentData/article_df.csv")
article_csv = spark.read.csv('assignmentData/article_df.csv', header=True)
comments_df.write.format("csv").mode("overwrite").option("header", "true").save("assignmentData/comments_df.csv")
comments_csv = spark.read.csv('assignmentData/comments_df.csv', header=True)

article_csv.show(5)
comments_csv.show(5)
