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