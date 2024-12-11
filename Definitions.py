from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
import requests

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
