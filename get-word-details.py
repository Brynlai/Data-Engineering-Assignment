!pip install google.generativeai

%env AIAPI=AIabcs....KEY
GEMINIAPI = %env AIAPI
print(GEMINIAPI)

import google.generativeai as genai
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType
from pyspark.sql.functions import udf, explode, col
from typing import List, Optional

# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()

def get_word_details(words):
    """
    Generate word details in CSV format for the given list of words.

    Args:
        words (list of str): List of words to process.

    Returns:
        str: CSV content as a string.
    """
    print("Start get_word_details")
    genai.configure(api_key=GEMINIAPI) # Replace with your actual API key

    # Create the model
    generation_config = {
        "temperature": 0.7,
        "top_p": 0.9,
        "top_k": 20,
        "max_output_tokens": 8192,  # Double the current limit if supported by the API
        "response_mime_type": "text/plain",
    }
    
    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash-8b",
        generation_config=generation_config,
    )

    chat_session = model.start_chat(
      history=[
      ]
    )
    
    # Improved prompt
    prompt = f"""You are a Labeling Machine. Generate in text a CSV file with the structure: "word,definition,antonym,synonym,tatabahasa,sentiment". 
    Rules:
    1. Definition must explain the word in Malay and not repeat the word.
    2. Antonyms and synonyms must be meaningful; use "tidak diketahui" if unavailable.
    3. Tatabahasa must be two Malay words like "kata nama".
    4. Sentiment is a string between "-1.0" and "1.0", neutral being "0.0".
    5. Enclose all values in double quotes.
    Example:
    "word","definition","antonym","synonym","tatabahasa","sentiment"
    "kami","kata ganti nama diri jamak, merujuk kepada penutur","mereka","kita","kata ganti","0.0"
    "gawgwah","tidak diketahui","tidak diketahui","tidak diketahui","tidak diketahui","0.0"
    
    Based on these Words, DO NOT SKIP ANY WORDS and generate the csv file: {', '.join(words)}
    """

    
    # Sending the prompt to the chat model
    response = chat_session.send_message(prompt)
    
    # Extract the text content from the response
    text_response = response.text
    
    # Clean up the output to remove any unnecessary characters or formatting
    text_response = (
        text_response
        .replace("```", "")
        .replace('"word","definition","antonym","synonym","tatabahasa","sentiment"\n', '')
    )
    
    print("get_word_details completed. Returned:")
    print(text_response)
    print("Ended get_word_details")
    
    return text_response



get_word_details(["whavig2yv2r"])



# Read input data
clean_words_df = spark.read.csv("assignData/clean_words_data_csv", header=True)
print("clean_words_df.count():", clean_words_df.count())

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Add a unique row number to the DataFrame
window = Window.orderBy("Cleaned_Word")  # Adjust orderBy as needed
clean_words_df = clean_words_df.withColumn("row_number", row_number().over(window))

# Batch processing
batch_size = 50
total_rows = clean_words_df.count()
all_csv_data = []

for i in range(0, total_rows, batch_size):
    # Select a specific batch of rows
    batch_df = clean_words_df.filter((col("row_number") > i) & (col("row_number") <= i + batch_size))
    batch_words = batch_df.select("Cleaned_Word").rdd.map(lambda row: row[0]).collect()
    
    # Call the get_word_details function with the batch
    batch_csv_data = get_word_details(batch_words)
    
    # Process the response
    batch_rows = batch_csv_data.strip().split("\n")
    if batch_rows[0].startswith('"word"'):
        batch_rows = batch_rows[1:]
    
    all_csv_data.extend(batch_rows)

# Parse rows into structured data
parsed_data = [row.split(',') for row in all_csv_data if len(row.split(',')) == 6]

# Define schema
schema = StructType([
    StructField("word", StringType(), True),
    StructField("definition", StringType(), True),
    StructField("antonym", StringType(), True),
    StructField("synonym", StringType(), True),
    StructField("tatabahasa", StringType(), True),
    StructField("sentiment", StringType(), True),
])

# Create DataFrame
all_csv_data_df = spark.createDataFrame(parsed_data, schema=schema)

# Save to CSV
output_path = "assignData/word_details_csv"
all_csv_data_df.write.option("header", True) \
                     .mode("overwrite") \
                     .csv(output_path)

print(f"Data written to {output_path}")
print(f"Number of usable word scsv_data_df : {csv_data_df.count()}")


from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType
word_details_csv = spark.read.csv("assignData/word_details_csv", header=True)
# Define UDF to filter out unusable words
def is_usable(definition):
    return "tidak diketahui" not in definition.lower() or "nama" not in definition.lower()

is_usable_udf = udf(is_usable, BooleanType())

# Filter usable words
cleaned_data = word_details_csv.filter(is_usable_udf(col("definition")))

# Save the cleaned data to a new CSV
cleaned_data.write.csv("assignData/word_details_csv_cleaned", header=True, mode="overwrite")

print(f"Number of usable words: {cleaned_data.count()}")

word_details_csv_cleaned = spark.read.csv("assignData/word_details_csv_cleaned", header=True)
print(f"Output of word_details_csv_cleaned.show(20): {word_details_csv_cleaned.show(20)}")
