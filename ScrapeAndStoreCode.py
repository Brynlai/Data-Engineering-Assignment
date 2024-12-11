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
print("article_csv_words: ")
article_csv_words.select("Content_Words").show(5)
print("comments_csv_words: ")
comments_csv_words.show(5)

from pyspark.sql import Row

# Extract words as an RDD
article_words_rdd = article_csv_words.select("Content_Words").rdd.flatMap(lambda x: x.Content_Words)
comment_words_rdd = comments_csv_words.select("Comment_Text_Words").rdd.flatMap(lambda x: x.Comment_Text_Words)

# Combine the RDDs of words
all_words_rdd = article_words_rdd.union(comment_words_rdd)

# Convert the combined RDD back to a DataFrame
all_words_df = all_words_rdd.map(lambda x: Row(word=x)).toDF(["word"])

print("Combined Words:")
all_words_df.show(6)
all_words_df.count()

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, StringType
import re

# Step 1: Remove duplicates
all_words_unique = all_words_df.dropDuplicates()

# Step 2: Remove punctuation from words
def clean_word(word):
    return re.sub(r'[^a-zA-Z]', '', word)

clean_word_udf = udf(clean_word, StringType())
cleaned_words_with_punctuation_removed = all_words_unique.withColumn("word", clean_word_udf("word"))

# Step 3: Filter out non-alphabetic characters (now that punctuation is removed)
def is_alphabetic(word):
    return word.isalpha()

is_alphabetic_udf = udf(is_alphabetic, BooleanType())
cleaned_words = cleaned_words_with_punctuation_removed.filter(is_alphabetic_udf("word"))

# Show the cleaned DataFrame
print("Cleaned Words:")
cleaned_words.show(20)
cleaned_words.count()

cleaned_words.write.format("csv").mode("overwrite").option("header", "true").save("assignData/all_words_cleaned_csv")
all_words_cleaned_csv = spark.read.csv('assignData/all_words_cleaned_csv', header=True)
