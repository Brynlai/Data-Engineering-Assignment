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
