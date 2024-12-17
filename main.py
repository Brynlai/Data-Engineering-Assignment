from wikipedia_api import fetch_search_results, fetch_page_content
from kafka_client import produce_to_kafka
from spark_cleaner import clean_and_process_stream

KAFKA_TOPIC = "wikipedia_topic"
KAFKA_SERVER = "localhost:9092"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/user/output/cleaned_words"

def main():
    # Step 1: Fetch search results from Wikipedia
    print("Fetching titles from Wikipedia...")
    titles = fetch_search_results(query="bahasa", limit=20)

    # Step 2: Fetch page content for each title
    print("Fetching page content...")
    contents = [fetch_page_content(title) for title in titles]
    print(f"Fetched content for {len(contents)} pages.")

    # Step 3: Produce content to Kafka
    print("Producing page content to Kafka...")
    produce_to_kafka(topic=KAFKA_TOPIC, messages=contents, bootstrap_servers=KAFKA_SERVER)

    # Step 4: Process Kafka data with Spark
    print("Starting Spark job for cleaning and processing...")
    clean_and_process_stream(
        topic=KAFKA_TOPIC,
        kafka_servers=KAFKA_SERVER,
        hdfs_output_path=HDFS_OUTPUT_PATH
    )
    print("Workflow complete.")

if __name__ == "__main__":
    main()
