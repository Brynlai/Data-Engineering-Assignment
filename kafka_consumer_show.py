from pyspark.sql.functions import from_json, col, regexp_replace, split, explode
from pyspark.sql.types import StructType, StructField, StringType
from GlobalSparkSession import global_spark_session
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaConsumer

def check_and_create_kafka_topic(topic_name, bootstrap_servers):
    """
    Checks if a Kafka topic exists, and creates it if it doesn't.

    This function uses the KafkaAdminClient to check if the specified Kafka topic exists.
    If the topic does not exist, it creates the topic with the given name.
    """
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        # Check if the topic exists
        topic_metadata = admin_client.list_topics()
        if topic_name not in topic_metadata:
            # Create the topic if it doesn't exist
            topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        else:
            print(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Error checking or creating topic '{topic_name}': {e}")
    finally:
        admin_client.close()

def kafka_consumer():
    """
    Consumes data from Kafka, processes it, and saves it to a CSV file.

    This function reads data from a Kafka topic, extracts title and content from JSON messages,
    cleans the content, splits it into words, filters out empty words, and saves the results
    to a CSV file. It also prints the filtered words to the console.
    """
    topic_name = "wiki_topic"
    bootstrap_servers = "localhost:9092"

    # Check and create Kafka topic if necessary
    check_and_create_kafka_topic(topic_name, bootstrap_servers)

    # Define the schema for the JSON messages
    schema = StructType([
        StructField("Title", StringType(), True),
        StructField("Content", StringType(), True)
    ])

    # Create Spark session
    spark = global_spark_session()

    # Read data from Kafka
    # Spark Structured Streaming starts here
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic_name) \
        .load()

    # Convert the value from binary to string
    json_df = df.selectExpr("CAST(value AS STRING) as json")

    # Extract Title and Content from JSON
    messages_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

    # Clean the content by removing non-alphabetic characters
    cleaned_df = messages_df.withColumn("Cleaned_Content", regexp_replace(col("Content"), "[^a-zA-Z ]", ""))

    # Split the cleaned content into individual words
    split_words_df = cleaned_df.withColumn("Words", split(col("Cleaned_Content"), " "))

    # Explode the list of words into individual rows
    exploded_words_df = split_words_df.select(explode(col("Words")).alias("Word"))

    # Filter out empty words
    filtered_words_df = exploded_words_df.filter(col("Word") != "")

    # Print the filtered words to the console
    query = filtered_words_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Save the filtered words to a CSV file
    query_to_csv = filtered_words_df.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("header", "true") \
        .option("path", "assignData/wiki_word_data_csv_test") \
        .option("checkpointLocation", "assignData/wiki_word_data_checkpoint_test") \
        .start()
    
    # Wait for both queries to terminate
    query.awaitTermination()
    query_to_csv.awaitTermination()

if __name__ == "__main__":
    kafka_consumer()
