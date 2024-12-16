from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import explode, split, col, regexp_replace
import requests
import os
import threading
import time
from typing import List, Tuple

def main():
    class WikipediaProcessor:
        def __init__(self, app_name: str):
            self.spark = SparkSession.builder.appName(app_name)\
                .config("spark.sql.streaming.schemaInference", "true")\
                .config("spark.sql.adaptive.enabled", "false")\
                .getOrCreate()

        def fetch_wikipedia_articles(self, aid_values: range) -> List[Tuple[int, str, str]]:
            url = "https://ms.wikipedia.org/w/api.php"
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
            articles_data = self.fetch_wikipedia_articles(aid_values)
            article_schema = StructType([
                StructField("ArticleID", IntegerType(), True),
                StructField("Title", StringType(), True),
                StructField("Content", StringType(), True),
            ])
            articles_df = self.spark.createDataFrame(articles_data, schema=article_schema)
            filtered_df = articles_df.filter((articles_df["Title"] != "") & (articles_df["Content"] != ""))
            articles_df_limited = filtered_df.select("Title", "Content").limit(50)
            print("Displaying the first 50 records:")
            articles_df_limited.show(truncate=False)
            articles_df_limited.write.format("csv").mode("overwrite").option("header", "true").save(output_path)
            print(f"Data saved to {output_path}")

        def tokenize_csv_content(self, input_path: str, output_path: str):
            articles_df = self.spark.read.format("csv").option("header", "true").load(input_path)
            if "Content" not in articles_df.columns or "Title" not in articles_df.columns:
                raise ValueError("Input CSV must contain 'Title' and 'Content' columns.")
            tokenized_df = articles_df.withColumn(
                "Cleaned_Word",
                explode(
                    split(
                        regexp_replace(col("Content"), r"[,\"\\'\\-]", ""),
                        "\\s+"
                    )
                )
            ).select("Title", "Cleaned_Word")
            print("Displaying tokenized content:")
            tokenized_df.show(truncate=False)
            tokenized_df.write.format("csv").mode("overwrite").option("header", "true").save(output_path)
            print(f"Tokenized data saved to {output_path}")

        def process_streaming_data(self, input_dir: str, output_dir: str, timeout: int = 10):
            streaming_df = self.spark.readStream\
                .format("csv")\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .load(input_dir)

            if "Title" not in streaming_df.columns or "Cleaned_Word" not in streaming_df.columns:
                raise ValueError("Input CSV must contain 'Title' and 'Cleaned_Word' columns.")

            processed_df = streaming_df.select("Title", "Cleaned_Word")
            query = processed_df.writeStream\
                .format("csv")\
                .option("path", output_dir)\
                .option("checkpointLocation", f"{output_dir}/checkpoint")\
                .outputMode("append")\
                .start()

            print(f"Streaming job started. Writing data to: {output_dir}")

            def stop_query_after_timeout():
                time.sleep(timeout)
                if query.isActive:
                    print(f"Stopping the query after {timeout} seconds.")
                    query.stop()

            stopper_thread = threading.Thread(target=stop_query_after_timeout)
            stopper_thread.start()
            query.awaitTermination()

            if os.path.exists(output_dir) and os.listdir(output_dir):
                print(f"Files saved successfully in {output_dir}.")
            else:
                print("Done")

    processor = WikipediaProcessor("CombinedWikipediaProcessor")

    aid_values = range(2000, 2100)
    article_output_path = "assignmentData/wikipedia_articles_top50.csv"
    tokenized_output_path = "assignmentData/wikipedia_articles_tokenized.csv"
    streaming_output_path = "assignmentData/Data_Streaming"

    # Process articles
    processor.process_articles(aid_values, article_output_path)

    # Tokenize CSV content
    processor.tokenize_csv_content(article_output_path, tokenized_output_path)

    # Process streaming data
    processor.process_streaming_data(tokenized_output_path, streaming_output_path, timeout=10)

if __name__ == "__main__":
    main()
