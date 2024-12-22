"""
Author  : Alia Tasnim binti Baco
Date    : 22/12/2024
"""

from pyspark.sql import Row
from pyspark.sql.functions import col, sum, desc, count, avg
from UtilsRedis import Redis_Utilities
from neo4j import GraphDatabase
from GlobalSparkSession import global_spark_session
import redis

# Retrieve all frequencies from Redis
redis_client = Redis_Utilities()  # Assuming UtilsRedis is your class handling Redis interactions
word_frequencies_dict = redis_client.get_word_frequencies()

# Convert the dictionary to a PySpark DataFrame
spark = global_spark_session()
word_frequencies_list = [{"Cleaned_Word": word, "Frequency": int(freq)} for word, freq in word_frequencies_dict.items()]
word_frequencies_df = spark.createDataFrame(Row(**x) for x in word_frequencies_list)

# Show the first 5 rows
print("Preview of all word frequencies (first 5 rows):")
word_frequencies_df.show(10)

# Retrieve any word to confirm frequency
#====================
# Most common words (Top 5)
most_common_words_df = word_frequencies_df.orderBy(col("Frequency").desc()).limit(10)
print("Most common words:")
most_common_words_df.show()

# Least common words (Bottom 5)
least_common_words_df = word_frequencies_df.orderBy(col("Frequency").asc()).limit(10)
print("Least common words:")
least_common_words_df.show()
