import json
from kafka import KafkaProducer
from UtilsWikipedia import fetch_search_results, fetch_page_content, extract_page_info

def kafka_producer():
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Fetch search results from Wikipedia API
    titles = fetch_search_results()

    # Fetch content for each title and produce to Kafka
    for title in titles:
        page_content = fetch_page_content(title)
        page_info_list = extract_page_info(page_content)
        for page_info in page_info_list:
            print(f"Producing message: {page_info}")
            producer.send('wiki_topic', page_info)

    # Flush and close the 
    producer.flush()
    producer.close()

if __name__ == "__main__":
    kafka_producer()