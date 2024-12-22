"""
Authors: Lai ZhonPoa, Alia
"""

from neo4j import GraphDatabase
import redis

class DataBaseHandler:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password, redis_client):
        self.neo4j_driver = self._setup_neo4j_driver(neo4j_uri, neo4j_user, neo4j_password)
        self.redis_client = redis_client

    def _setup_neo4j_driver(self, uri, user, password):
        """
        Initialize and verify a Neo4j driver connection.
        """
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        return driver

    def insert_word_data(self, word, definition, tatabahasa, synonym, antonym):
        """
        Insert word data into Neo4j with relationships.
        """
        with self.neo4j_driver.session() as session:
            session.write_transaction(self._insert_into_neo4j, word, definition, tatabahasa, synonym, antonym)

    @staticmethod
    def _insert_into_neo4j(tx, word, definition, tatabahasa, synonym, antonym):
        # Insert or update the original word
        tx.run(
            "MERGE (w:Word {word: $word}) "
            "SET w.definition = $definition, w.tatabahasa = $tatabahasa",
            word=word, definition=definition, tatabahasa=tatabahasa
        )
        # Insert or update the synonym and create relationship if valid
        if synonym != "tidak diketahui" and synonym != word:
            tx.run(
                "MERGE (s:Word {word: $synonym}) "
                "SET s.definition = $definition, s.tatabahasa = $tatabahasa "
                "WITH s "
                "MATCH (w:Word {word: $word}) "
                "MERGE (w)-[:SYNONYM]->(s)",
                word=word, synonym=synonym, definition=definition, tatabahasa=tatabahasa
            )

        # Insert or update the antonym and create relationship without setting definition and tatabahasa
        if antonym != "tidak diketahui" and antonym != word:
            tx.run(
                "MERGE (a:Word {word: $antonym}) "
                "WITH a "
                "MATCH (w:Word {word: $word}) "
                "MERGE (w)-[:ANTONYM]->(a)",
                word=word, antonym=antonym
            )

    def populate_database(self, data):
        """
        Insert data into Neo4j and Redis.
        """
        for row in data:
            #print(f"Populating: {row}")
            word = row['word'].strip('"')
            definition = row['definition'].strip('"')
            antonym = row['antonym'].strip('"')
            synonym = row['synonym'].strip('"')
            tatabahasa = row['tatabahasa'].strip('"')
            
            try:
                sentiment = float(row['sentiment'].strip('"'))
            except ValueError:
                print(f"Skipping row with invalid sentiment data for word: {word}")
                continue
            self.insert_word_data(word, definition, tatabahasa, synonym, antonym)

            """
            Author: Alia Tasnim Binti Baco
            """
            self.redis_client.store_sentiment(word, sentiment)
            self.redis_client.update_tatabahasa_count(tatabahasa)
            self.redis_client.update_sentiment_count(sentiment)

    def get_total_unique_entries(self):
        """
        Get the total number of unique entries (words, phrases) in the lexicon.
        """
        with self.neo4j_driver.session() as session:
            result = session.read_transaction(self._count_unique_entries)
            return result

    @staticmethod
    def _count_unique_entries(tx):
        result = tx.run("MATCH (w:Word) RETURN count(DISTINCT w.word) AS unique_entries")
        return result.single()["unique_entries"]

    def get_synonyms(self, word):
        """
        Retrieve synonyms for a given word.
        """
        query = """
        MATCH (w:Word {word: $word})-[:SYNONYM]->(synonym:Word)
        RETURN synonym.word AS synonym
        """
        with self.neo4j_driver.session() as session:
            result = session.run(query, word=word)
            return [record["synonym"] for record in result]

    def get_antonyms(self, word):
        """
        Retrieve antonyms for a given word.
        """
        query = """
        MATCH (w:Word {word: $word})-[:ANTONYM]->(antonym:Word)
        RETURN antonym.word AS antonym
        """
        with self.neo4j_driver.session() as session:
            result = session.run(query, word=word)
            return [record["antonym"] for record in result]