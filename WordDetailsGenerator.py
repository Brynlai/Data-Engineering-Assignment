"""
Author: Lai ZhonPoa, Alia Tasnim Binti Baco
"""
import google.generativeai as genai
from typing import List

class WordDetailsGenerator:
    @staticmethod
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
            "top_p": 0.92, # Ensures words with 92% or more cumulative probability are considered.
        
            # Top-k: Restricts word selection to the top-k most probable words.
            "top_k": 30, # Limits selection to the top 20 most probable words.
        
            # Max Output Tokens: Specifies the maximum number of tokens the model can generate.
            "max_output_tokens": 8192, # Allows for longer responses.
        
            # Response MIME Type: Defines the format of the response data.
            "response_mime_type": "text/plain", # Ensures the response is in plain text format.
        }
    
    
        # Select llm model and config
        model = genai.GenerativeModel(
            model_name="gemini-1.5-flash",
            generation_config=generation_config,
        )
    
        chat_session = model.start_chat()
        
        # Improved prompt 360 tokens
        prompt = f"""
        You are an expert in Bahasa Malaysia and Mixed Malay, tasked with creating a structured lexicon. Generate a CSV file in text format with the following structure:
        "word","definition","antonym","synonym","tatabahasa","sentiment"
        
        Rules:
        1. Provide a clear and concise definition in Malay, without repeating the word itself.
        2. Antonyms and synonyms:
           - Antonyms must contrast conceptually with the word's meaning and cannot have definitions identical to the word.
           - Synonyms must share a similar meaning and context.
           - Use "tidak diketahui" if no meaningful antonym or synonym is available or if the word lacks contextual alternatives.
        3. Tatabahasa (part of speech) must be concise and accurate, using standard Malay grammar terms like "kata nama."
        4. Sentiment must be a numerical string between "-1.0" and "1.0," where "0.0" represents neutral sentiment.
        5. Enclose all values in double quotes, and ensure each row has unique and distinct values across columns.
        6. For non-conventional words (e.g., numbers or mixed-language terms), provide "tidak diketahui" for antonym and synonym.
        
        Example:
        "word","definition","antonym","synonym","tatabahasa","sentiment"
        "kami","kata ganti nama diri jamak, merujuk kepada penutur","mereka","kita","kata ganti","0.0"
        "gembira","rasa senang hati atau bahagia","sedih","bahagia","kata sifat","0.9"
        
        Based on the provided words, generate the CSV file content without skipping any words: {', '.join(words)}
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
        
        print("get_word_details completed. \n<Start>")
        print(text_response)
        print("<Stop> Ended get_word_details")
        
        return text_response
