!pip install google.generativeai


import google.generativeai as genai
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType
from pyspark.sql.functions import udf, explode, col
from typing import List, Optional

# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()

genai.configure(api_key="API key from https://aistudio.google.com/prompts/new_chat") # Replace with your actual API key

# Create the model
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
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

def get_word_details(words):
    print("Start get_word_details")
    prompt = f"""Generate a CSV file with the following structure: "word,definition,antonym,synonym,tatabahasa,sentiment". Definition must be explanation and cannot be the same as word. Tatabahasa can only be 2 words. All the rows and columns should be in string format enclosed with "". Sentiment should be values -1 to 1 enclosed with ""..
    
    For each word, provide a definition, an antonym, and a synonym, all in Malay. Ensure the CSV output uses double quotes around each string value. Do not include any introductory or explanatory text outside of the CSV data.
    Words to process: {', '.join(words)}
    """
    response = chat_session.send_message(prompt)
    
    # Extract the text content
    text_response = response.text

    # remove "```" and ""word","definition","antonym","synonym","tatabahasa","sentiment"\n"
     # remove "```" and ""word","definition","antonym","synonym","tatabahasa","sentiment"\n"
    text_response = text_response.replace("```", "").replace('"word","definition","antonym","synonym","tatabahasa","sentiment"\n', '')
    print("get_word_details completed. Returned:", text_response)
    print("Ended get_word_details")
    return text_response



get_word_details(["kami"])


if __name__ == '__main__':
    # Read input data
    clean_words_df = spark.read.csv("assignData/clean_words_data_csv", header=True)
    print("clean_words_df.count():", clean_words_df.count())

    batch_size = 100
    all_csv_data = []
    print("1. for i in range(0, clean_words_df.count(), batch_size):")
    # Process words in batches
    for i in range(0, clean_words_df.count(), batch_size):
        batch_words = clean_words_df.select("Cleaned_Word").rdd.map(lambda row: row[0]).take(batch_size)
        batch_csv_data = get_word_details(batch_words)
        
        # Split response into lines and remove the header if present
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
                         .option("quoteAll", True) \
                         .option("escape", "\"") \
                         .mode("overwrite") \
                         .csv(output_path)

    print(f"Data written to {output_path}")

word_details_csv = spark.read.csv("assignData/word_details_csv", header=True)
word_details_csv.show(20)
