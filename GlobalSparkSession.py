"""
Author: Lai ZhonPoa
"""

from pyspark.sql import SparkSession

# Initialize SparkSession
def global_spark_session():
    """
    Creates or retrieves a SparkSession.

    This function initializes a SparkSession if one doesn't already exist, or retrieves the existing one.
    It's designed to be called globally to ensure only one SparkSession is active in the application.

    Returns:
        SparkSession: The active SparkSession instance.
    """
    return SparkSession.builder \
        .appName("data-engineering-project") \
        .getOrCreate()
