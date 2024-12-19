from UtilsNeo4J import setup_neo4j_driver, insert_into_neo4j, populate_database
from UtilsRedis import Redis_Update_Count
import redis
# Initialize SparkSession
from GlobalSparkSession import global_spark_session
spark = global_spark_session()

# Load data from the cleaned CSV file
word_details_csv_cleaned = spark.read.csv("assignData/word_details_csv_cleaned_test", header=True)
# Convert the Spark DataFrame to a list of Rows for easier processing
data = word_details_csv_cleaned.collect()
print(len(data))  # Use len() to get the count of elements in the list


# Setup Neo4j driver
driver = setup_neo4j_driver(
    uri="neo4j+s://abc.databases.neo4j.io",
    user="neo4j",
    password="abc"  # Remember to replace with your actual password!
)

# Create Instance
redis_handler = Redis_Update_Count()

# Populate the Neo4j database with the data
populate_database(driver, redis_handler, data)

# Query Neo4j to verify the number of nodes created
with driver.session() as session:
    query = "MATCH (n) RETURN count(n) as totalNodes"
    result = session.run(query)
    for record in result:
        print(f"Total Nodes: {record['totalNodes']}")
