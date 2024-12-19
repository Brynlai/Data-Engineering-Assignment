from neo4j import GraphDatabase
import redis

def setup_neo4j_driver(uri, user, password):
    """
    Initialize and verify a Neo4j driver connection.

    Args:
        uri (str): The URI of the Neo4j database.
        user (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        neo4j.Driver: The Neo4j driver instance.
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    return driver

def insert_into_neo4j(tx, word, definition, tatabahasa, synonym, antonym, sentiment):
    """
    Insert word data into Neo4j with relationships.

    Args:
        tx (neo4j.Transaction): The transaction object.
        word (str): The word to insert.
        definition (str): The definition of the word.
        tatabahasa (str): The grammatical category of the word.
        synonym (str): A synonym of the word.
        antonym (str): An antonym of the word.
        sentiment (float): The sentiment score of the word.
    """
    tx.run(
        "MERGE (w:Word {word: $word}) "
        "SET w.definition = $definition, w.tatabahasa = $tatabahasa, w.sentiment = $sentiment",
        word=word, definition=definition, tatabahasa=tatabahasa, sentiment=sentiment
    )

    # Create synonym relationship
    if synonym != "tidak diketahui" and synonym != word:
        tx.run(
            "MERGE (s:Word {word: $synonym}) "
            "SET s.definition = $definition, s.tatabahasa = $tatabahasa, s.sentiment = $sentiment "  # Ensure synonym node has properties
            "WITH s "
            "MATCH (w:Word {word: $word}) "
            "MERGE (w)-[:SYNONYM]->(s)",
            word=word, synonym=synonym, definition=definition, tatabahasa=tatabahasa, sentiment=sentiment
        )

    # Create antonym relationship
    if antonym != "tidak diketahui" and antonym != word:
        tx.run(
            "MERGE (a:Word {word: $antonym}) "
            "SET a.definition = $definition, a.tatabahasa = $tatabahasa, a.sentiment = $sentiment " # Ensure antonym node has properties
            "WITH a "
            "MATCH (w:Word {word: $word}) "
            "MERGE (w)-[:ANTONYM]->(a)",
            word=word, antonym=antonym, definition=definition, tatabahasa=tatabahasa, sentiment=sentiment
        )

def populate_database(driver, redis_util, data):
    """
    Insert data into Neo4j and Redis.

    Args:
        driver (neo4j.Driver): The Neo4j driver instance.
        redis_client (redis.StrictRedis): The Redis client instance.
        data (list of dict): The data to insert, where each dictionary represents a row.
    """
    with driver.session() as session:
        print("Populating Neo4J")
        for row in data:
            print(f"Populating: {row}")
            word = row['word'].strip('"')
            definition = row['definition'].strip('"')
            antonym = row['antonym'].strip('"')
            synonym = row['synonym'].strip('"')
            tatabahasa = row['tatabahasa'].strip('"')
            
            try:
              sentiment = float(row['sentiment'].strip('"'))
            except ValueError:
                print(f"Skipping row with invalid sentiment data for word: {word}, sentiment {sentiment}")
                continue

            # Insert into Neo4j
            session.write_transaction(insert_into_neo4j, word, definition, tatabahasa, synonym, antonym, sentiment)

            # Store and Update in Redis
            redis_util.store_sentiment(word, sentiment)
            redis_util.update_tatabahasa_count(tatabahasa)
            redis_util.update_sentiment_count(sentiment)

def count_unique_entries(tx):
    """
    Count the total number of unique entries (words, phrases) in the lexicon.

    Args:
        tx (neo4j.Transaction): The transaction object.

    Returns:
        int: The total number of unique entries.
    """
    result = tx.run("MATCH (w:Word) RETURN count(DISTINCT w.word) AS unique_entries")
    return result.single()["unique_entries"]
    

def get_total_unique_entries(driver):
    """
    Get the total number of unique entries (words, phrases) in the lexicon.

    Args:
        driver (neo4j.Driver): The Neo4j driver instance.

    Returns:
        int: The total number of unique entries.
    """
    with driver.session() as session:
        return session.read_transaction(count_unique_entries)




def get_synonyms(driver, word):
    """
    Retrieve synonyms for a given word.

    Args:
        driver (neo4j.Driver): The Neo4j driver instance.
        word (str): The word to search for.

    Returns:
        list: A list of synonyms.
    """
    query = """
    MATCH (w:Word {word: $word})-[:SYNONYM]->(synonym:Word)
    RETURN synonym.word AS synonym
    """
    with driver.session() as session:
        result = session.run(query, word=word)
        return [record["synonym"] for record in result]

def get_antonyms(driver, word):
    """
    Retrieve antonyms for a given word.

    Args:
        driver (neo4j.Driver): The Neo4j driver instance.
        word (str): The word to search for.

    Returns:
        list: A list of antonyms.
    """
    query = """
    MATCH (w:Word {word: $word})-[:ANTONYM]->(antonym:Word)
    RETURN antonym.word AS antonym
    """
    with driver.session() as session:
        result = session.run(query, word=word)
        return [record["antonym"] for record in result]



def get_word_sentiment(driver, word):
    """
    Retrieve the sentiment of a word from Neo4j.

    Args:
        driver (neo4j.Driver): The Neo4j driver instance.
        word (str): The word to search for.

    Returns:
        float: The sentiment value of the word, or None if not found.
    """
    with driver.session() as session:
        result = session.run(
            "MATCH (w:Word {word: $word}) RETURN w.sentiment AS sentiment",
            word=word
        )
        record = result.single()
        if record:
            return record["sentiment"]
        else:
            return None
