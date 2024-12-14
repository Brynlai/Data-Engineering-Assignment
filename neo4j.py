from pyspark.sql import SparkSession

# PySpark setup
spark = SparkSession.builder \
    .appName("Populate Neo4j") \
    .getOrCreate()

word_details_csv_cleaned = spark.read.csv("assignData/word_details_csv_cleaned", header=True)
print(f"Output of word_details_csv_cleaned.show(20): {word_details_csv_cleaned.show(20)}")
data = word_details_csv_cleaned.collect()


from neo4j import GraphDatabase

URI = "neo4j+s://1234.databases.neo4j.io" ### !!!! REMEMBER TO DELETE AND REPLACE
neo4j_user = "neo4j"
neo4j_password = "1234-1234-1234"  ###  !!!! REMEMBER TO DELETE AND REPLACE

driver = GraphDatabase.driver(URI, auth=(neo4j_user, neo4j_password))

driver.verify_connectivity()

import redis
redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)

# Function to insert data into Neo4j
def insert_into_neo4j(tx, word, definition, tatabahasa, synonym, antonym):
    # Create the word node with properties
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


# Insert data into Neo4j and Redis
def process_data():
    with driver.session() as session:
        for row in data:
            word = row['word'].strip('"')
            definition = row['definition'].strip('"')
            antonym = row['antonym'].strip('"')
            synonym = row['synonym'].strip('"')
            tatabahasa = row['tatabahasa'].strip('"')
            sentiment = float(row['sentiment'].strip('"'))

            # Insert into Neo4j
            session.write_transaction(insert_into_neo4j, word, definition, tatabahasa, synonym, antonym)

            # Insert into Redis
            redis_client.hset(f"word:{word}", mapping={
                "sentiment": sentiment
            })

process_data()
print("Data successfully inserted into Neo4j and Redis!")


with driver.session() as session:
    query = "MATCH (n) RETURN count(n) as totalNodes"
    result = session.run(query)
    for record in result:
        print(f"Total Nodes: {record['totalNodes']}")
