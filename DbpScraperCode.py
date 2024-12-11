from bs4 import BeautifulSoup
import requests

class WordInfo:
    def __init__(self, word):
        self.word = word
        self.definitions = None
        self.synonyms = None
        self.antonyms = None

    def __str__(self):
        return f"Word: {self.word}\nDefinitions:\n{self.definitions}\nSynonyms: {self.synonyms}\nAntonyms: {self.antonyms}"

class WebScraper:
    def __init__(self, url_template):
        self.url_template = url_template

    def fetch_page(self, word):
        url = self.url_template.format(word)
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.text
        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def parse_definitions(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        definition_section = soup.find("div", {"class": "tab-content"})
        if definition_section:
            definitions = []
            for tab_pane in definition_section.find_all("div", class_="tab-pane"):
                if tab_pane.get('id'):
                    definition_text = tab_pane.find("b", string="Definisi : ")
                    if definition_text:
                        definitions.append(definition_text.next_sibling.strip())
            return "\n".join(definitions) if definitions else None
        return None

    def parse_synonyms(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        thesaurus_section = soup.find("table", class_="info")
        
        if thesaurus_section:
            synonyms = []
            
            for row in thesaurus_section.find_all("tr"):
                cells = row.find_all("td")
                for cell in cells:
                    text = cell.get_text(strip=True)
                    
                    # Find Bersinonim dengan {word}
                    if "Bersinonim dengan" in text and not synonyms:
                        start_index = text.find("Bersinonim dengan") + len("Bersinonim dengan")
                        end_index = text.find("2.", start_index) if "2." in text[start_index:] else len(text)
                        if end_index == len(text):  # If no '2.' is found, use the entire text
                            end_index = len(text)
                        synonym_text = text[start_index:end_index].strip()
                        synonyms.extend(synonym_text.split(", "))
                        break  # Break out of the loop after finding the first set of synonyms
            
            return ", ".join(synonyms) if synonyms else None
        
        return None

    def parse_antonyms(self, html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        thesaurus_section = soup.find("table", class_="info")
        
        if thesaurus_section:
            antonyms = []
            
            for row in thesaurus_section.find_all("tr"):
                cells = row.find_all("td")
                for cell in cells:
                    text = cell.get_text(strip=True)
                    
                    # Find Berantonim dengan {word}
                    if "Berantonim dengan" in text and not antonyms:
                        start_index = text.find("Berantonim dengan") + len("Berantonim dengan")
                        end_index = text.find("<", start_index) if "<" in text[start_index:] else len(text)
                        antonym_text = text[start_index:end_index].strip()
                        antonyms.extend(antonym_text.split(", "))
                        break  # Break out of the loop after finding the first set of antonyms
            
            return ", ".join(antonyms) if antonyms else None
        
        return None

# Example usage of WebScraper class methods.
def main():
    url_template = "https://prpm.dbp.gov.my/Cari1?keyword={}"
    scraper = WebScraper(url_template)

    word_info = WordInfo("kami")

    html_content = scraper.fetch_page(word_info.word)
    
    if html_content is not None:
        word_info.definitions = scraper.parse_definitions(html_content)
        word_info.synonyms = scraper.parse_synonyms(html_content)
        word_info.antonyms = scraper.parse_antonyms(html_content)

        print(word_info)

main()
