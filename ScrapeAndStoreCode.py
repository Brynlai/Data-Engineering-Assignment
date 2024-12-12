from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql.functions import udf
from typing import List, Optional
from Classes import Scraped_Data, Comment
from Scrapes import scrape_article

# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()

# Base URL and AID range
base_url = "https://b.cari.com.my/portal.php?mod=view&aid="
aid_values = list(range(1, 1000))  # Adjust range as needed

# UDF to scrape article and comments
def scrape_data_udf(aid: int) -> Optional[tuple]:
    url = f"{base_url}{aid}"
    try:
        print(f"Scraping AID: {aid}")
        scraped_data = scrape_article(url, aid)
        if scraped_data:
            article = scraped_data[:-1]  # Exclude comments
            comments = scraped_data[-1]  # Extract comments
            return article, comments
        return None
    except Exception as e:
        print(f"Error scraping AID {aid}: {e}")
        return None

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
    .option("quoteAll", True) \
    .option("escape", "\"") \
    .csv("assignData/articles_data_csv")

comments_df.write.option("header", True) \
    .option("quoteAll", True) \
    .option("escape", "\"") \
    .csv("assignData/comments_data_csv")


from pyspark.sql.functions import udf, split, col, concat, explode

article_csv = spark.read.csv('assignData/articles_data_csv', header=True)
comments_csv = spark.read.csv('assignData/comments_data_csv', header=True)

print("article_csv.count(): ", article_csv.count())
print("comments_csv.count(): ", comments_csv.count())

article_csv.count()
filtered_article_csv = article_csv.filter((col("views").cast("int").isNotNull()) & (col("views") > 0))
filtered_article_csv.count()

# Concatenate Title and Content columns and then split into words
article_csv_words = filtered_article_csv.withColumn("Combined_Text", concat(col("Title"), col("Content"))) \
                                       .withColumn("Combined_Words", split(col("Combined_Text"), " "))

comments_csv_words = comments_csv.withColumn("Comment_Text_Words", split(col("Comment_Text"), " "))

print("article_csv_words: ")
article_csv_words.show(5)
print("comments_csv_words: ")
comments_csv_words.show(5)

# Clean individual Words
print("article_csv_words: ")
article_csv_words.select("Combined_Words").show(5)
print("comments_csv_words: ")
comments_csv_words.show(5)


print("Article Words  Count:", article_csv_words.count())
print("Article Words  :", article_csv_words.show(50, truncate=True))
print("Comment Words  Count:", comments_csv_words.count())
print("Comment Words  :", comments_csv_words.show(50, truncate=True))
print(type(article_csv_words))

combined_words_df = article_csv_words.select(explode("Combined_Words").alias("Word"))
# Show the first few rows to verify
print("Combined Words:")
combined_words_df.show(50, truncate=True)
print("Combined Words Count:", combined_words_df.count())




# Define a UDF to convert words to lowercase and remove non-alphabetic characters
def clean_word(word):
    return ''.join(char for char in word.lower() if char.isalpha())

# Register the UDF
clean_word_udf = udf(clean_word, StringType())

# Apply the UDF to the 'Word' column
cleaned_combined_words_df = combined_words_df.withColumn("Cleaned_Word", clean_word_udf("Word"))

# Remove duplicates based on 'Cleaned_Word'
distinct_cleaned_words_df = cleaned_combined_words_df.select("Cleaned_Word").distinct()

# Show the first few rows to verify
print("Distinct Cleaned Combined Words:")
distinct_cleaned_words_df.show(50, truncate=True)
print("Distinct Cleaned Combined Words Count:", distinct_cleaned_words_df.count())


distinct_cleaned_words_df.write.option("header", True) \
    .option("quoteAll", True) \
    .option("escape", "\"") \
    .csv("assignData/clean_words_data_csv")


