from neo4j import GraphDatabase
from googletrans import Translator
import csv
from datetime import datetime

class Neo4jLexiconManager:
    """
    Class to manage Neo4j operations for the lexicon database.
    """
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def validate_csv(self, csv_path):
        required_fields = ['Malay Word', 'Definition', 'Part of Speech', 'Sentiment', 
                           'Synonym', 'Antonym', 'Hypernym', 'Hyponym', 'Meronyms', 'Holonyms']
        with open(csv_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            if not all(field in reader.fieldnames for field in required_fields):
                raise ValueError(f"CSV must contain the following fields: {required_fields}")

    def word_exists(self, word):
        query = "MATCH (w:Word {word: $word}) RETURN w.word AS word"
        with self._driver.session() as session:
            result = session.run(query, word=word)
            return result.single() is not None

    def upsert_word_data(self, tx, word_data):
        relationships = {
            'Synonym': 'HAS_SYNONYM',
            'Antonym': 'HAS_ANTONYM',
            'Hypernym': 'HAS_HYPERNYM',
            'Hyponym': 'HAS_HYPONYM',
            'Meronyms': 'HAS_MERONYM',
            'Holonyms': 'HAS_HOLONYM'
        }
        # Upsert word node
        tx.run(
            """
            MERGE (w:Word {word: $word})
            SET w.definition = $definition,
                w.part_of_speech = $part_of_speech,
                w.sentiment = $sentiment
            """,
            word=word_data['Malay Word'],
            definition=word_data['Definition'],
            part_of_speech=word_data['Part of Speech'],
            sentiment=word_data['Sentiment']
        )
        # Handle relationships
        for key, relationship in relationships.items():
            if word_data[key]:
                related_words = word_data[key].split(", ")
                for related_word in related_words:
                    if related_word != word_data['Malay Word']:
                        tx.run(
                            f"""
                            MERGE (w:Word {{word: $word}})
                            MERGE (r:Word {{word: $related_word}})
                            MERGE (w)-[:{relationship}]->(r)
                            """,
                            word=word_data['Malay Word'],
                            related_word=related_word
                        )

    def load_and_update_lexicon(self, csv_path):
        self.validate_csv(csv_path)
        with self._driver.session() as session:
            with open(csv_path, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    session.write_transaction(self.upsert_word_data, row)
        print("Lexicon successfully updated in Neo4j!")

    def find_word(self, word):
        query = """
        MATCH (w:Word {word: $word})-[r]-(related:Word)
        RETURN w.word AS Word, type(r) AS Relationship, related.word AS RelatedWord
        """
        with self._driver.session() as session:
            result = session.run(query, word=word)
            for record in result:
                print(f"Word: {record['Word']}, Relationship: {record['Relationship']}, Related Word: {record['RelatedWord']}")

    def manual_upsert_word(self, word_data):
        with self._driver.session() as session:
            session.write_transaction(self.upsert_word_data, word_data)
        print(f"Word '{word_data['Malay Word']}' successfully updated.")

    def correct_word(self, current_word, new_word):
        if not self.word_exists(current_word):
            print(f"Error: The word '{current_word}' does not exist in the database.")
            return

        if self.word_exists(new_word):
            print(f"Error: The word '{new_word}' already exists in the database.")
            return

        query = """
        MATCH (w:Word {word: $current_word})
        SET w.word = $new_word
        """
        with self._driver.session() as session:
            session.run(query, current_word=current_word, new_word=new_word)
        print(f"Word '{current_word}' successfully corrected to '{new_word}'.")

    def validate_relationships(self, word):
        query = """
        MATCH (w:Word {word: $word})-[r]->(related)
        RETURN type(r) AS Relationship, related.word AS RelatedWord
        """
        with self._driver.session() as session:
            result = session.run(query, word=word)
            inconsistencies = []
            for record in result:
                if record['RelatedWord'] == word:
                    inconsistencies.append(record['Relationship'])
            if inconsistencies:
                print(f"Inconsistencies found: {inconsistencies}")

    def log_update(self, word, action):
        with open("lexicon_update_log.txt", "a") as log_file:
            log_file.write(f"{word} | {action} | {datetime.now()}\n")

class TranslatorService:
    """
    Class to handle word translations using Google Translate.
    """
    def __init__(self):
        self.translator = Translator()

    def translate_word(self, word):
        try:
            translation = self.translator.translate(word, src='ms', dest='en')
            print(f"Translation for '{word}': {translation.text}")
            return translation.text
        except Exception as e:
            print(f"Error translating '{word}': {e}")
            return input("Enter translation manually: ")

class LexiconCLI:
    """
    Command-Line Interface to interact with the Neo4j Lexicon Manager.
    """
    def __init__(self, neo4j_manager, translator):
        self.neo4j_manager = neo4j_manager
        self.translator = translator

    def manual_update_lexicon(self):
        while True:
            print("\nManual Lexicon Update")
            word = input("Enter Malay Word (or type 'exit' to quit): ")
            if word.lower() == 'exit':
                break

            translation = self.translator.translate_word(word)
            definition = input("Enter Definition: ")
            part_of_speech = input("Enter Part of Speech: ")
            sentiment = input("Enter Sentiment: ")
            synonym = input("Enter Synonyms: ")
            antonym = input("Enter Antonyms: ")
            hypernym = input("Enter Hypernyms: ")
            hyponym = input("Enter Hyponyms: ")
            meronyms = input("Enter Meronyms: ")
            holonyms = input("Enter Holonyms: ")

            word_data = {
                'Malay Word': word,
                'Definition': definition,
                'Part of Speech': part_of_speech,
                'Sentiment': sentiment,
                'Synonym': synonym,
                'Antonym': antonym,
                'Hypernym': hypernym,
                'Hyponym': hyponym,
                'Meronyms': meronyms,
                'Holonyms': holonyms
            }
            self.neo4j_manager.manual_upsert_word(word_data)

    def correct_lexicon_error(self):
        while True:
            print("\nLexicon Error Correction")
            current_word = input("Enter the word to correct (or type 'exit' to quit): ")
            if current_word.lower() == 'exit':
                break
            new_word = input("Enter the corrected word: ")
            self.neo4j_manager.correct_word(current_word, new_word)

    def main_menu(self):
        while True:
            print("\n========================================================")
            print("         Welcome to our Lexicon Maintenance and Update :)      ")
            print("========================================================")
            print("1. Find word relationships")
            print("2. Manually update lexicon")
            print("3. Correct lexicon errors")
            print("4. Exit")
            choice = input("Choose an option: ")
            if choice == '1':
                word = input("Enter the word to search: ")
                self.neo4j_manager.find_word(word)
            elif choice == '2':
                self.manual_update_lexicon()
            elif choice == '3':
                self.correct_lexicon_error()
            elif choice == '4':
                print("Exiting... Goodbye!")
                break


