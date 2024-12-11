from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
import requests

# Initialize SparkSession
spark = SparkSession.builder.appName("DBP Scraping").getOrCreate()

def fetch_definition(word):
    url = f"https://prpm.dbp.gov.my/Cari1?keyword={word}"
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        definition_section = soup.find("div", {"class": "tab-content"})
        if definition_section:
            definitions = definition_section.text.strip()
            return word, definitions
    except Exception as e:
        return word, None

# Input list of words to scrape
words_rdd = spark.sparkContext.parallelize(["sebab", "contoh", "ajar","takut","dapat","baginda"])
definitions_rdd = words_rdd.map(fetch_definition)
valid_definitions = definitions_rdd.filter(lambda x: x[1] is not None)

# Convert to DataFrame
df = spark.createDataFrame(valid_definitions, ["Word", "Definition"])


path = "assignment_storage/word-definition"
# If the file does not exist, write the new data
df.write.mode("overwrite").format("parquet").save(path)
