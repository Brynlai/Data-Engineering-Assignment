from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql.functions import udf, explode, col
from typing import List, Optional
from Classes import Scraped_Data, Comment
from Scrapes import scrape_article
from WebScraper import WebScraper, WordInfo

# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()



# Read cleaned words
clean_words_df = spark.read.csv("assignData/clean_words_data_csv", header=True)

# UDF to fetch word details
def fetch_word_details(word: str) -> Optional[tuple]:
    try:
        print(f"Fetching details for word: {word}")
        url_template = "https://prpm.dbp.gov.my/Cari1?keyword={}"
        scraper = WebScraper(url_template)
        word_info = WordInfo(word)

        html_content = scraper.fetch_page(word)
        if not html_content:
            print(f"No HTML content returned for word: {word}")
            return None

        word_info.definitions = scraper.parse_definitions(html_content)
        word_info.synonyms = scraper.parse_synonyms(html_content)
        word_info.antonyms = scraper.parse_antonyms(html_content)

        return (word_info.word, word_info.definitions, word_info.synonyms, word_info.antonyms)
    except Exception as e:
        print(f"Error fetching details for word '{word}': {e}")
        return None

# Registering the UDF
word_details_udf = udf(fetch_word_details, StructType([
    StructField("Word", StringType(), True),
    StructField("Definitions", StringType(), True),
    StructField("Synonyms", StringType(), True),
    StructField("Antonyms", StringType(), True),
]))

# Apply UDF to fetch word details
word_details_df = clean_words_df.withColumn("WordDetails", word_details_udf(col("Cleaned_Word"))).selectExpr(
    "WordDetails.Word AS Word",
    "WordDetails.Definitions AS Definitions",
    "WordDetails.Synonyms AS Synonyms",
    "WordDetails.Antonyms AS Antonyms"
)

word_details_df.show(10)



# Write enriched data to CSV
word_details_df.write.option("header", True) \
    .option("quoteAll", True) \
    .option("escape", "\"") \
    .csv("assignData/word_details_csv")



word_details_csv = spark.read.csv("assignData/word_details_csv", header=True)
word_details_csv.show(10)
