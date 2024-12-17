from wikipedia_api import fetch_search_results, fetch_page_content, extract_page_info
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, explode
from pyspark.sql.types import StructType, StructField, StringType

def get_cleaned_words_df():
    # Create a Spark Session
    print("Creating Spark Session...")
    spark = SparkSession.builder.appName("WordExtractor").getOrCreate()

    # Fetch search results from Wikipedia API
    print("Fetching search results from Wikipedia API...")
    titles = fetch_search_results()
    print(f"Fetched titles: {titles}")

    # Create a DataFrame from the titles
    print("Creating DataFrame from titles...")
    titles_df = spark.createDataFrame([(title,) for title in titles], ["Title"])

    # Define a schema for the page content DataFrame
    schema = StructType([
        StructField("Title", StringType(), True),
        StructField("Content", StringType(), True)
    ])

    # Function to fetch and extract content for each title
    def fetch_and_extract_content(title):
        print(f"Fetching content for title: {title}")
        page_content = fetch_page_content(title)
        extracted_info = extract_page_info(page_content)
        return extracted_info

    # Apply the fetch_and_extract_content function to each title and flatten the result
    print("Applying fetch_and_extract_content function to each title...")
    rdd = titles_df.rdd.flatMap(lambda row: fetch_and_extract_content(row["Title"]))
    all_content_df = spark.createDataFrame(rdd, schema)
    print("Fetched and extracted content for all titles.")

    # Clean the content by removing non-alphabetic characters
    print("Cleaning content to remove non-alphabetic characters...")
    cleaned_words_df = all_content_df.withColumn("Cleaned_Words", regexp_replace(all_content_df["Content"], "[^a-zA-Z ]", ""))
    
    # Split the cleaned content into individual words
    print("Splitting cleaned content into individual words...")
    split_words_df = cleaned_words_df.withColumn("Words", split(cleaned_words_df["Cleaned_Words"], " "))
    
    # Explode the list of words into individual rows and filter out empty words
    print("Exploding list of words and filtering out empty words...")
    individual_words_df = split_words_df.select(explode("Words").alias("Cleaned_Word"))
    individual_words_df = individual_words_df.filter(individual_words_df["Cleaned_Word"] != "")

    print("Returning DataFrame of cleaned words.")
    return individual_words_df