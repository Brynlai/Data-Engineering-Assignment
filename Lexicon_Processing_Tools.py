from neo4j import GraphDatabase
from googletrans import Translator
import csv
from datetime import datetime
import os


class LexiconDBManager:
    """
    Class to handle operations with Neo4j for the lexicon database.
    """
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def disconnect(self):
        self.driver.close()

    def check_csv_format(self, file_path):
        expected_columns = ['Malay Word', 'Definition', 'Part of Speech', 'Sentiment',
                            'Synonym', 'Antonym', 'Hypernym', 'Hyponym', 'Meronyms', 'Holonyms']
        with open(file_path, mode='r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            if not all(column in reader.fieldnames for column in expected_columns):
                raise ValueError(f"The CSV file must have the following columns: {expected_columns}")

    def is_word_present(self, word):
        query = "MATCH (w:Word {word: $word}) RETURN w.word AS word"
        with self.driver.session() as session:
            result = session.run(query, word=word)
            return result.single() is not None

    def is_label_present(self, label):
        query = f"MATCH (n:{label}) RETURN COUNT(n) AS count"
        with self.driver.session() as session:
            result = session.run(query)
            count = result.single()["count"]
            return count > 0

    def add_or_update_word(self, tx, word_info):
        relationship_mapping = {
            'Synonym': 'SYNONYM',
            'Antonym': 'ANTONYM',
            'Hypernym': 'HYPERNYM',
            'Hyponym': 'HYPONYM',
            'Meronyms': 'MERONYM',
            'Holonyms': 'HOLONYM'
        }
        tx.run(
            """
            MERGE (w:LexiconWord {word: $word})
            SET w.definition = $definition,
                w.part_of_speech = $part_of_speech,
                w.sentiment = $sentiment
            """,
            word=word_info['Malay Word'],
            definition=word_info['Definition'],
            part_of_speech=word_info['Part of Speech'],
            sentiment=word_info['Sentiment']
        )
        for key, rel in relationship_mapping.items():
            if word_info[key]:
                related_words = word_info[key].split(", ")
                for related_word in related_words:
                    if related_word != word_info['Malay Word']:
                        tx.run(
                            f"""
                            MERGE (w:LexiconWord {{word: $word}})
                            MERGE (related:LexiconWord {{word: $related_word}})
                            MERGE (w)-[:{rel}]->(related)
                            """,
                            word=word_info['Malay Word'],
                            related_word=related_word
                        )

    def update_lexicon_from_csv(self, file_path):
        self.check_csv_format(file_path)
        if not self.is_label_present("LexiconWord"):
            print("Label 'LexiconWord' does not exist in the database. Creating initial node...")
            with self.driver.session() as session:
                session.run("CREATE (w:LexiconWord {word: 'example', definition: 'example definition'})")
        with self.driver.session() as session:
            with open(file_path, mode='r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file)
                for row in reader:
                    session.write_transaction(self.add_or_update_word, row)
        print("Lexicon database updated successfully!")

    def search_word(self, word):
        query = """
        MATCH (w:Word {word: $word})-[r]-(related:Word)
        RETURN w.word AS Word, type(r) AS Relationship, related.word AS RelatedWord
        """
        with self.driver.session() as session:
            result = session.run(query, word=word)
            for record in result:
                print(f"Word: {record['Word']}, Relationship: {record['Relationship']}, Related Word: {record['RelatedWord']}")

    def update_word_manually(self, word_info):
        with self.driver.session() as session:
            session.write_transaction(self.add_or_update_word, word_info)
        print(f"Word '{word_info['Malay Word']}' has been updated.")

    def fix_word_entry(self, current_word, corrected_word):
        if not self.is_word_present(current_word):
            print(f"The word '{current_word}' does not exist.")
            return
        if self.is_word_present(corrected_word):
            print(f"The word '{corrected_word}' already exists.")
            return
            
        query = """
        MATCH (w:Word {word: $current_word})
        SET w.word = $corrected_word
        """
        with self.driver.session() as session:
            session.run(query, current_word=current_word, corrected_word=corrected_word)
        print(f"Word '{current_word}' successfully corrected to '{corrected_word}'.")


class WordTranslator:
    """
    Class for translating words using Google Translate.
    """
    def __init__(self):
        self.google_translator = Translator()

    def translate_to_english(self, word):
        try:
            translation = self.google_translator.translate(word, src='ms', dest='en')
            print(f"Translation: '{word}' -> '{translation.text}'")
            return translation.text
        except Exception as e:
            print(f"Translation error: {e}")
            return input("Enter the English translation manually: ")


class LexiconInterface:
    """
    User Interface to manage lexicon updates and interactions.
    """
    def __init__(self, db_manager, translator):
        self.db_manager = db_manager
        self.translator = translator

    def interactive_word_update(self):
        while True:
            word = input("Enter a Malay word to update (or 'exit' to quit): ")
            if word.lower() == 'exit':
                break
            translation = self.translator.translate_to_english(word)
            definition = input("Definition: ")
            pos = input("Part of Speech: ")
            sentiment = input("Sentiment: ")
            synonym = input("Synonyms (comma-separated): ")
            antonym = input("Antonyms (comma-separated): ")
            hypernym = input("Hypernyms (comma-separated): ")
            hyponym = input("Hyponyms (comma-separated): ")
            meronym = input("Meronyms (comma-separated): ")
            holonym = input("Holonyms (comma-separated): ")

            word_info = {
                'Malay Word': word,
                'Definition': definition,
                'Part of Speech': pos,
                'Sentiment': sentiment,
                'Synonym': synonym,
                'Antonym': antonym,
                'Hypernym': hypernym,
                'Hyponym': hyponym,
                'Meronyms': meronym,
                'Holonyms': holonym
            }
            self.db_manager.update_word_manually(word_info)

    def correct_word_entry(self):
        while True:
            current_word = input("Word to correct (or 'exit' to quit): ")
            if current_word.lower() == 'exit':
                break
            corrected_word = input("Correct word: ")
            self.db_manager.fix_word_entry(current_word, corrected_word)

    def run(self):
        while True:
            print("\n ************************************************************** ")
            print("    Welcome To Use Our Lexicon Maintenance and Update System :)  ")
            print("*****************************************************************")
            print("\n1. Search Word Relationships")
            print("2. Add or Update Word Manually")
            print("3. Correct Error Word")
            print("4. Exit")
            choice = input("Choose an option: ")
            if choice == '1':
                word = input("Enter word to search: ")
                self.db_manager.search_word(word)
            elif choice == '2':
                self.interactive_word_update()
            elif choice == '3':
                self.correct_word_entry()
            elif choice == '4':
                print("Goodbye!")
                break
