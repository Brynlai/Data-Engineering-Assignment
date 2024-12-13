!pip install google.generativeai
import google.generativeai as genai
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql.functions import udf, explode, col
from typing import List, Optional

# PySpark setup
spark = SparkSession.builder \
    .appName("ScrapedDataProcessor") \
    .getOrCreate()

genai.configure(api_key="API key from  https://aistudio.google.com/prompts/new_chat") # Replace with your actual API key

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
    prompt = f"""Generate a CSV file with the following structure: "word,definition,antonym,synonym,tatabahasa,sentiment". Sentiment should be values -1 to 1.
    
    For each word, provide a definition, an antonym, and a synonym, all in Malay. Ensure the CSV output uses double quotes around each string value. Do not include any introductory or explanatory text outside of the CSV data.
    
    Words to process: {', '.join(words)}
    """
    
    response = chat_session.send_message(prompt)
    return response.text


if __name__ == '__main__':
  clean_words_df = spark.read.csv("assignData/clean_words_data_csv", header=True)
  print("clean_words_df.count(): ",clean_words_df.count())
  batch_size = 100

  all_csv_data = "" 

  for i in range(0, len(clean_words_df.collect()), batch_size):
    batch_words = [row.Cleaned_Word for row in clean_words_df.rdd.repartition(1).take(batch_size)]
    batch_csv_data = get_word_details(batch_words)
    all_csv_data += batch_csv_data 

  # Split the string into a list of rows
  all_csv_data_rows = all_csv_data.strip().split('\n') 

  # Create a list of lists from the rows
  all_csv_data_list = [row.split(',') for row in all_csv_data_rows]

  # Create a Spark DataFrame
  all_csv_data_df = spark.createDataFrame(all_csv_data_list, ["word", "definition", "antonym", "synonym", "tatabahasa", "sentiment"]) 
  all_csv_data_df.write.option("header", True) \
                     .option("quoteAll", True) \
                     .option("escape", "\"") \
                     .mode("overwrite") \
                     .csv("assignData/word_details_csv")


all_csv_data_df.show(1000)
