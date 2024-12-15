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
