from neo4j import GraphDatabase
import redis

def setup_neo4j_driver(uri, user, password):
    """Initialize and verify a Neo4j driver connection."""
    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    return driver

def insert_into_neo4j(tx, word, definition, tatabahasa, synonym, antonym):
    """Insert word data into Neo4j with relationships."""
    tx.run(
        "MERGE (w:Word {word: $word}) "
        "SET w.definition = $definition, w.tatabahasa = $tatabahasa",
        word=word, definition=definition, tatabahasa=tatabahasa
    )

    # Create synonym relationship
    if synonym != "tidak diketahui" and synonym != word:
        tx.run(
            "MERGE (s:Word {word: $synonym}) "
            "SET s.definition = $definition, s.tatabahasa = $tatabahasa "
            "WITH s "
            "MATCH (w:Word {word: $word}) "
            "MERGE (w)-[:SYNONYM]->(s)",
            word=word, synonym=synonym, definition=definition, tatabahasa=tatabahasa
        )

    # Create antonym relationship
    if antonym != "tidak diketahui" and antonym != word:
        tx.run(
            "MERGE (a:Word {word: $antonym}) "
            "SET a.definition = $definition, a.tatabahasa = $tatabahasa "
            "WITH a "
            "MATCH (w:Word {word: $word}) "
            "MERGE (w)-[:ANTONYM]->(a)",
            word=word, antonym=antonym, definition=definition, tatabahasa=tatabahasa
        )

def populate_database(driver, redis_client, data):
    """Insert data into Neo4j and Redis."""
    with driver.session() as session:
        print("Populating Neo4J")
        for row in data:
            word = row['word'].strip('"')
            definition = row['definition'].strip('"')
            antonym = row['antonym'].strip('"')
            synonym = row['synonym'].strip('"')
            tatabahasa = row['tatabahasa'].strip('"')
            sentiment = float(row['sentiment'].strip('"'))

            # Insert into Neo4j
            session.write_transaction(insert_into_neo4j, word, definition, tatabahasa, synonym, antonym)

            redis_client.hset(f"word:{word}", mapping={
                "sentiment": sentiment
            })

