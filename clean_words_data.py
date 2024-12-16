import redis  # Import Redis
from UtilsCleaner import process_words, save_word_frequencies_to_redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CleanWordsDataProcessor") \
    .getOrCreate()

# Load data from assignmentData/Data_Streaming
input_path = "assignmentData/Data_Streaming"
article_csv_words = spark.read.format("csv") \
    .option("header", "true") \
    .load(input_path)

# Inspect the DataFrame schema to confirm available columns
print("Schema of the loaded DataFrame:")
article_csv_words.printSchema()

# Use the correct column (e.g., Cochabamba0) and split into individual words
combined_words_df = article_csv_words.select(explode(split("Cochabamba0", "\\s+")).alias("Word"))

# Show the first few rows to verify
print("Combined Words:")
combined_words_df.show(10, truncate=True)
print("Combined Words Count:", combined_words_df.count())

# Initialize Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# Process words using UtilsCleaner
distinct_words_df = process_words(spark, combined_words_df, redis_client)

# Write the DataFrame to CSV
output_path = "assignmentData/clean_words_data_csv"
distinct_words_df.write.option("header", True) \
    .mode("overwrite") \
    .option("quoteAll", True) \
    .option("escape", "\"") \
    .csv(output_path)

print(f"Processed data saved to {output_path}")

# Stop Spark session
spark.stop()
