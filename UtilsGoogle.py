import google.generativeai as genai
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from typing import List

def get_word_details(words: List[str], gemini_api_key: str) -> str:
    """
    Generate word details in CSV format for the given list of words.

    Args:
        words (list of str): List of words to process.

    Returns:
        str: CSV content as a string.
    """
    print("Start get_word_details")
    genai.configure(api_key=gemini_api_key) # Replace with your actual API key


    # Create the model with specific generation configuration
    generation_config = {
        # Temperature: Controls the randomness and creativity of the generated text.
        "temperature": 0.9, # Balances determinism and creativity.
    
        # Top-p (Nucleus Sampling): Sets a cumulative probability threshold for word selection.
        "top_p": 0.90, # Ensures words with 90% or more cumulative probability are considered.
    
        # Top-k: Restricts word selection to the top-k most probable words.
        "top_k": 20, # Limits selection to the top 20 most probable words.
    
        # Max Output Tokens: Specifies the maximum number of tokens the model can generate.
        "max_output_tokens": 8192, # Allows for longer responses.
    
        # Response MIME Type: Defines the format of the response data.
        "response_mime_type": "text/plain", # Ensures the response is in plain text format.
    }


    # Select llm model and config
    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash-8b",
        generation_config=generation_config,
    )

    chat_session = model.start_chat()
    
    # Improved prompt
    prompt = f"""You are a very generalized consistent Labeling Machine specializing in Bahasa Malaysia and Mixed Malay. Generate in text a CSV file with the structure: "word,definition,antonym,synonym,tatabahasa,sentiment". 
    Rules:
    1. Definition must explain the word in Malay and not repeat the word.
    2. Antonyms and synonyms must be meaningful; use "tidak diketahui" if unavailable.
    3. Tatabahasa must be the most concise Malay words like "kata nama".
    4. Sentiment is a string between "-1.0" and "1.0", neutral being "0.0".
    5. Enclose all values in double quotes.
    6. In each row, each column of that row must not be the same.
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
