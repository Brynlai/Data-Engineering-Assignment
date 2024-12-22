"""
Author  : Xavier Ngow Kar Yuen
Date    : 22/12/2024
"""
from pyspark.sql import Row
from pyspark.sql.functions import col, countDistinct
from UtilsRedis import Redis_Utilities
from neo4j import GraphDatabase
from GlobalSparkSession import GlobalSparkSession
import redis
import matplotlib.pyplot as plt
# Initialize Spark session
spark = GlobalSparkSession.get_instance()
