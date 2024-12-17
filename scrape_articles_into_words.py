from UtilsProcessor import ScrapedDataProcessor, WordDetailsProcessor
from ForumScraper import scrape_data_udf
from UtilsCleaner import process_words
from UtilsGoogle import get_word_details
import google.generativeai as genai

# Initialize ScrapedDataProcessor
scraped_data_processor = ScrapedDataProcessor()
scraped_data_processor.setup_udf(scrape_data_udf)

# === 1. Data Collection and preperation ===
# === * cari.com.my and wikipedia api ===
# Define AID values
aid_values = list(range(44, 45))

# Process articles and comments
article_df, comments_df = scraped_data_processor.process_articles(aid_values)
scraped_data_processor.save_dataframes(article_df, comments_df)

# Process words
scraped_combined_words_df = scraped_data_processor.process_words('assignData/articles_data_csv_test', 'assignData/comments_data_csv_test')
# crawled_data = class.functionToGetCrawledWords()
# scraped_and_crawled_words = combine(scraped_combined_words_df , crawled_data)
scraped_data_processor.save_cleaned_words(scraped_combined_words_df, process_words)



# === 2. Lexicon Creation ===
# === 3. Lexicon Enrichment ===
# === * Definition, Antonym, Synonym, Tatabahasa, Sentiment ===
# Initialize WordDetailsProcessor
gemini_api = 'fwaf'  # Replace with your actual Gemini API key
word_details_processor = WordDetailsProcessor(gemini_api)

# Read and process clean words
clean_words_df = word_details_processor.read_clean_words('assignData/clean_words_data_csv_test')
clean_words_df = word_details_processor.add_row_number(clean_words_df)

# Batch process word details: Word, Definition, Antonym, Synonym, Tatabahasa, Sentiment.
all_csv_data = word_details_processor.batch_process(clean_words_df, 50, get_word_details)
word_details_processor.parse_and_save(all_csv_data, 'assignData/word_details_csv_test')

# Filter usable words
word_details_processor.filter_usable_words('assignData/word_details_csv_test', 'assignData/word_details_csv_cleaned_test')
