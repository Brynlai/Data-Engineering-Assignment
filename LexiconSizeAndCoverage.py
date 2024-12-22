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

class LexiconBenchmarkAnalysis:
    def __init__(self, spark):
        self.spark = spark

    def compare_to_benchmark(self, lexicon_size, benchmark_size):
        """
        Compare lexicon size to a benchmark.
        """
        coverage_percentage = (lexicon_size / benchmark_size) * 100 if benchmark_size > 0 else 0
        return {
            "Lexicon_Size": lexicon_size,
            "Benchmark_Size": benchmark_size,
            "Coverage_Percentage": coverage_percentage
        }

    @staticmethod
    def visualize_benchmark_coverage(results):
        """
        Visualize the benchmark coverage as a pie chart.
        """
        labels = ['Covered', 'Not Covered']
        values = [
            results["Lexicon_Size"],
            max(0, results["Benchmark_Size"] - results["Lexicon_Size"])
        ]

        colors = ["blue", "lightgray"]
        explode = (0.1, 0)  # Slightly offset the "Covered" segment

        plt.figure(figsize=(8, 8))
        plt.pie(
            values,
            labels=labels,
            autopct='%1.1f%%',
            colors=colors,
            explode=explode,
            shadow=True,
            startangle=140
        )
        plt.title("Lexicon Coverage Compared to Benchmark", fontsize=16)
        plt.legend(labels, loc="upper left")
        plt.show()

# Example usage
if __name__ == "__main__":
    # Initialize Spark session
    spark = GlobalSparkSession.get_instance()

    # Load the lexicon data
    lexicon_df = spark.read.csv("assignData/word_details_csv_cleaned_test", header=True, inferSchema=True)

    # Calculate lexicon size
    lexicon_size = lexicon_df.count()

    # Define a benchmark size (e.g., 100,000 as the benchmark vocabulary size)
    benchmark_size = 100000

    # Perform the analysis
    analysis = LexiconBenchmarkAnalysis(spark)
    benchmark_results = analysis.compare_to_benchmark(lexicon_size, benchmark_size)

    # Print the results
    print("Lexicon Benchmark Analysis Results:")
    print(f"Lexicon Size: {benchmark_results['Lexicon_Size']}")
    print(f"Benchmark Size: {benchmark_results['Benchmark_Size']}")
    print(f"Coverage Percentage: {benchmark_results['Coverage_Percentage']:.2f}%")

    # Visualize the coverage
    analysis.visualize_benchmark_coverage(benchmark_results)
