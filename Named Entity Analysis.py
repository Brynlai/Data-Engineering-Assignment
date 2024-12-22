from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.broadcast import Broadcast
from neo4j import GraphDatabase
import spacy
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NamedEntityAnalysis") \
    .master("local[*]") \
    .getOrCreate()

from UtilsRedis import Redis_Utilities

redis_handler = Redis_Utilities(host="localhost", port=6379, db=0)

neo4j_connection_data = {
    "uri": "neo4j+s://42ed6f07.databases.neo4j.io",
    "user": "neo4j",
    "password": "BMUxKQwZ0Ijfs5goSEaHysuahJ9Tyikiq4m6kW9m3I",
}
# Broadcast the spaCy NLP model
nlp_broadcast = spark.sparkContext.broadcast(spacy.load("en_core_web_sm"))

#Broadcast spaCy NLP Model
nlp_broadcast = spark.sparkContext.broadcast(spacy.load("en_core_web_sm"))
neo4j_handler_broadcast = spark.sparkContext.broadcast(neo4j_connection_data)

# Define the Named Entity Analysis class
class AdvancedNamedEntityAnalysis:
    def __init__(self, spark, nlp_model: Broadcast, neo4j_data=None):
        self.spark = spark
        self.nlp_model = nlp_model
        self.neo4j_data = neo4j_data

    def named_entity_analysis(self, lexicon_df):
        # Extract unique words from the DataFrame
        words_rdd = lexicon_df.select("word").distinct().rdd.map(lambda row: row.word)

        # Perform Named Entity Recognition
        def extract_entities(word, nlp_model):
            doc = nlp_model(word)
            return [(ent.text, ent.label_) for ent in doc.ents]

        # Deserialize spaCy model on workers
        nlp_model_value = self.nlp_model.value
        entities_rdd = words_rdd.flatMap(lambda word: extract_entities(word, nlp_model_value))
        entities_df = self.spark.createDataFrame(entities_rdd, ["Entity", "Type"])

        return entities_df

    def store_entities_in_neo4j(self, entities_df):
        def store_entity(tx, entity, entity_type):
            query = """
            MERGE (e:NamedEntity {name: $entity})
            SET e.type = $type
            """
            tx.run(query, entity=entity, type=entity_type)

        with GraphDatabase.driver(
            self.neo4j_data["uri"],
            auth=(self.neo4j_data["user"], self.neo4j_data["password"])
        ) as driver:
            with driver.session() as session:
                for row in entities_df.collect():
                    session.write_transaction(store_entity, row["Entity"], row["Type"])

# Load dataset
lexicon_df = spark.read.csv(
    "file:///home/student/de-assignment/de-venv/assignData/combined_word_details.csv",
    header=True,
    inferSchema=True
)

# Initialize analyzer
analyzer = AdvancedNamedEntityAnalysis(spark, nlp_broadcast, neo4j_connection_data)

# Perform Named Entity Analysis
print("Performing Named Entity Analysis...")
entities_df = analyzer.named_entity_analysis(lexicon_df)

# Display the analysis results
print("Named Entity Analysis Results:")
entities_df.show(truncate=False)

# Store entities in Neo4j
print("Storing entities in Neo4j...")
analyzer.store_entities_in_neo4j(entities_df)

# Visualize entity distribution
print("Visualizing entity distribution...")
entity_counts = entities_df.groupBy("Type").count().orderBy("count", ascending=False)
entity_counts.show()

# Collect data for plotting
data = entity_counts.collect()
labels = [row["Type"] for row in data]
sizes = [row["count"] for row in data]

# Plot the entity distribution
plt.figure(figsize=(10, 6))
plt.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=140)
plt.title("Distribution of Named Entity Types")
plt.axis("equal")
plt.show()
