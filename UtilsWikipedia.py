from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import requests
from typing import List, Tuple

class WikipediaDataProcessor:
    def __init__(self, app_name: str):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def fetch_wikipedia_articles(self, aid_values: range) -> List[Tuple[int, str, str]]:
        url = "https://ms.wikipedia.org/w/api.php"  # Bahasa Melayu Wikipedia API endpoint
        articles = []
        print("Fetching Wikipedia articles...")
        for aid in aid_values:
            params = {
                "action": "query",
                "format": "json",
                "pageids": aid,
                "prop": "extracts",
                "explaintext": True
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                pages = data.get("query", {}).get("pages", {})
                for page_id, page in pages.items():
                    title = page.get("title", "").strip()
                    content = page.get("extract", "").strip()
                    if title and content:
                        articles.append((int(page_id), title, content))
                        print(f"Fetched: {title}")
            except Exception as e:
                print(f"Failed to fetch article for AID {aid}: {e}")
        print("Fetching completed.")
        return articles

    def process_articles(self, aid_values: range, output_path: str):
        # Fetch articles
        articles_data = self.fetch_wikipedia_articles(aid_values)

        # Define schema
        article_schema = StructType([
            StructField("ArticleID", IntegerType(), True),
            StructField("Title", StringType(), True),
            StructField("Content", StringType(), True),
        ])

        # Create DataFrame
        articles_df = self.spark.createDataFrame(articles_data, schema=article_schema)

        # Filter out rows with empty Title or Content
        filtered_df = articles_df.filter(
            (articles_df["Title"] != "") & (articles_df["Content"] != "")
        )

        # Select top 50 articles
        articles_df_limited = filtered_df.select("Title", "Content").limit(50)

        # Show the first 50 records
        print("Displaying the first 50 records:")
        articles_df_limited.show(truncate=False)

        # Save to CSV
        articles_df_limited.write.format("csv").mode("overwrite").option("header", "true").save(output_path)
        print(f"Data saved to {output_path}")

# Main function
def main():
    processor = WikipediaDataProcessor("WikipediaDataProcessor")
    aid_values = range(2000, 2100)  # Adjust range as needed
    output_path = "assignmentData/wikipedia_articles_top50.csv"
    processor.process_articles(aid_values, output_path)

if __name__ == "__main__":
    main()




from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, regexp_replace

class WikipediaCSVProcessor:
    def __init__(self, app_name: str):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def tokenize_csv_content(self, input_path: str, output_path: str):
        # Read the CSV file into a DataFrame
        articles_df = self.spark.read.format("csv").option("header", "true").load(input_path)

        # Ensure necessary columns are present
        if "Content" not in articles_df.columns or "Title" not in articles_df.columns:
            raise ValueError("Input CSV must contain 'Title' and 'Content' columns.")

        # Remove unwanted symbols and tokenize into individual words
        tokenized_df = articles_df.withColumn(
            "Cleaned_Word",
            explode(
                split(
                    regexp_replace(col("Content"), r"[,\"\'\\-]", ""),  # Remove specific symbols: , " ' -
                    "\\s+"  # Split by whitespace
                )
            )
        ).select("Title", "Cleaned_Word")

        # Show tokenized content
        print("Displaying tokenized content:")
        tokenized_df.show(truncate=False)

        # Save the tokenized content to a new CSV file
        tokenized_df.write.format("csv").mode("overwrite").option("header", "true").save(output_path)
        print(f"Tokenized data saved to {output_path}")

# Main function
def main():
    processor = WikipediaCSVProcessor("WikipediaCSVProcessor")
    input_path = "assignmentData/wikipedia_articles_top50.csv"  # Path to the input CSV file
    output_path = "assignmentData/wikipedia_articles_tokenized.csv"  # Path to save the tokenized CSV file
    processor.tokenize_csv_content(input_path, output_path)

if __name__ == "__main__":
    main()




from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, regexp_replace
import os

class WikipediaStreamingProcessor:
    def __init__(self, app_name: str):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()

    def process_streaming_data(self, input_dir: str, output_dir: str):
        # Read the streaming data from the input directory
        streaming_df = self.spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(input_dir)

        # Ensure necessary columns are present
        if "Title" not in streaming_df.columns or "Cleaned_Word" not in streaming_df.columns:
            raise ValueError("Input CSV must contain 'Title' and 'Cleaned_Word' columns.")

        # Perform further processing if needed (e.g., filtering, transformations)
        processed_df = streaming_df.select("Title", "Cleaned_Word")

        # Write the streaming results to the output directory
        query = processed_df.writeStream \
            .format("csv") \
            .option("path", output_dir) \
            .option("checkpointLocation", f"{output_dir}/checkpoint") \
            .outputMode("append") \
            .start()

        print(f"Streaming job started. Writing data to: {output_dir}")

        # Block and wait for the streaming to finish
        query.awaitTermination()

        # Verify if files are written to the output directory
        if os.path.exists(output_dir) and os.listdir(output_dir):
            print(f"Files saved successfully in {output_dir}.")
        else:
            print(f"No files were saved in {output_dir}.")

# Main function
def main():
    processor = WikipediaStreamingProcessor("WikipediaStreamingProcessor")
    input_dir = "assignmentData/wikipedia_articles_tokenized.csv"  # Directory for tokenized input
    output_dir = "assignmentData/Data_Streaming"  # Directory to save streaming output
    processor.process_streaming_data(input_dir, output_dir)

if __name__ == "__main__":
    main()




