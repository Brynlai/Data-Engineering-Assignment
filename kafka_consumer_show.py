"""
Authors: Lim Zhao Qing, Xavier Ngow Kar Yuen
"""
from pyspark.sql.functions import from_json, col, regexp_replace, split, explode
from pyspark.sql.types import StructType, StructField, StringType
from GlobalSparkSession import GlobalSparkSession
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaConsumer

class KafkaConsumerShow:
    def __init__(self, topic_name, bootstrap_servers):
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers
        self.spark = GlobalSparkSession.get_instance()

    def check_and_create_kafka_topic(self):
        """
        Checks if a Kafka topic exists, and creates it if it doesn't.
        """
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        try:
            topic_metadata = admin_client.list_topics()
            if self.topic_name not in topic_metadata:
                topic_list = [NewTopic(name=self.topic_name, num_partitions=1, replication_factor=1)]
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
                print(f"Topic '{self.topic_name}' created successfully.")
            else:
                print(f"Topic '{self.topic_name}' already exists.")
        except Exception as e:
            print(f"Error checking or creating topic '{self.topic_name}': {e}")
        finally:
            admin_client.close()

    def kafka_consumer(self):
        """
        Consumes data from Kafka, processes it, and saves it to a CSV file.
        """
        self.check_and_create_kafka_topic()

        schema = StructType([
            StructField("Title", StringType(), True),
            StructField("Content", StringType(), True)
        ])

        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.topic_name) \
            .load()

        json_df = df.selectExpr("CAST(value AS STRING) as json")
        messages_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
        cleaned_df = messages_df.withColumn("Cleaned_Content", regexp_replace(col("Content"), "[^a-zA-Z ]", ""))
        split_words_df = cleaned_df.withColumn("Words", split(col("Cleaned_Content"), " "))
        exploded_words_df = split_words_df.select(explode(col("Words")).alias("Word"))
        filtered_words_df = exploded_words_df.filter(col("Word") != "")

        query = filtered_words_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query_to_csv = filtered_words_df.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("header", "true") \
            .option("path", "assignData/wiki_word_data_csv_test") \
            .option("checkpointLocation", "assignData/wiki_word_data_checkpoint_test") \
            .start()
        
        query.awaitTermination()
        query_to_csv.awaitTermination()

if __name__ == "__main__":
    consumer = KafkaConsumerShow(topic_name="wiki_topic", bootstrap_servers="localhost:9092")
    consumer.kafka_consumer()