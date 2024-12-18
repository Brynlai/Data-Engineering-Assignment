from pyspark.sql import SparkSession

# Initialize SparkSession
def global_spark_session():
    return SparkSession.builder \
        .appName("data-engineering-project") \
        .getOrCreate()
