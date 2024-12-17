from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace, split, explode

def consume_from_kafka_and_clean():
    spark = SparkSession.builder.appName("WikipediaDataCleaner").getOrCreate()
    
    # Read from Kafka topic using readStream
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "wikipedia_topic") \
        .load()
    
    # Convert value column to string (since it's binary)
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING) AS value")
    
    # Clean and process data (simple example: remove non-alphabetic characters)
    cleaned_df = kafka_df.withColumn("Cleaned_Value", regexp_replace(kafka_df["value"], "[^a-zA-Z ]", ""))
    
    # Split cleaned value into individual words
    split_df = cleaned_df.withColumn("Words", split(cleaned_df["Cleaned_Value"], " "))
    
    # Explode words into separate rows
    individual_words_df = split_df.select(explode("Words").alias("Word"))
    
    # Filter out empty strings
    individual_words_df = individual_words_df.filter(individual_words_df["Word"] != "")
    
    # Write cleaned data to file using writeStream
    query = individual_words_df.writeStream.outputMode("append").format("text").option("path", "/output/path").start()
    
    query.awaitTermination()