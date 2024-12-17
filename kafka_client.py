from kafka import KafkaProducer
from typing import List

def produce_to_kafka(topic: str, messages: List[str], bootstrap_servers: str = "localhost:9092"):
    """
    Produces a list of messages to a Kafka topic.
    """
    print(f"Connecting to Kafka at: {bootstrap_servers}")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8")
    )

    print(f"Producing {len(messages)} messages to Kafka topic '{topic}'...")
    for index, message in enumerate(messages):
        producer.send(topic, value=message)
    producer.flush()
    print(f"Successfully sent {len(messages)} messages to Kafka topic '{topic}'.")
    producer.close()
