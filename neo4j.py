from pyspark.sql import SparkSession
from UtilsNeo4J import setup_neo4j_driver, insert_into_neo4j, populate_database
from UtilsRedis import Redis_Util
import redis

# PySpark setup
spark = SparkSession.builder \
    .appName("Populate Neo4j") \
    .getOrCreate()

# Load data from the cleaned CSV file
word_details_csv_cleaned = spark.read.csv("assignData/word_details_csv_cleaned_test", header=True)
# Convert the Spark DataFrame to a list of Rows for easier processing
data = word_details_csv_cleaned.collect()
print(f"data: {word_details_csv_cleaned.show(20)}")




# Setup Neo4j driver
driver = setup_neo4j_driver(
    uri="neo4j+s://abc.databases.neo4j.io",
    user="neo4j",
    password="abc"  # Remember to replace with your actual password!
)

# Setup Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# Populate the Neo4j database with the data
populate_database(driver, redis_client, data)

# Query Neo4j to verify the number of nodes created
with driver.session() as session:
    query = "MATCH (n) RETURN count(n) as totalNodes"
    result = session.run(query)
    for record in result:
        print(f"Total Nodes: {record['totalNodes']}")
