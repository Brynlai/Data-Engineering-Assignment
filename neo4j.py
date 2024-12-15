from pyspark.sql import SparkSession
from UtilsNeo4J import setup_neo4j_driver, insert_into_neo4j, populate_database
import redis
# PySpark setup
spark = SparkSession.builder \
    .appName("Populate Neo4j") \
    .getOrCreate()

# Load data
word_details_csv_cleaned = spark.read.csv("assignData/word_details_csv_cleaned", header=True)
data = word_details_csv_cleaned.collect()
print(f"Output of word_details_csv_cleaned.show(20): {word_details_csv_cleaned.show(20)}")

# Setup Neo4j and Redis
driver = setup_neo4j_driver(
    uri="neo4j+s://f2d488e8.av.neo4j.io",
    user="neo4j",
    password="va"  # Remember to replace!
)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# populate_database with data
populate_database(driver, redis_client, data)

# Query Neo4j for total nodes
with driver.session() as session:
    query = "MATCH (n) RETURN count(n) as totalNodes"
    result = session.run(query)
    for record in result:
        print(f"Total Nodes: {record['totalNodes']}")
