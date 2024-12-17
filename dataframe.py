from pyspark.sql import SparkSession

def load_csv_to_dataframe(file_path: str):
    """
    Load a CSV file into a PySpark DataFrame.
    
    :param file_path: Path to the CSV file.
    :return: PySpark DataFrame containing the data from the CSV file.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("LoadCSV") \
        .getOrCreate()
    
    # Load the CSV file into a DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    return df

# Example usage
file_path = "assignmentData/clean_words_data_csv"
df = load_csv_to_dataframe(file_path)

# Show the DataFrame (for testing purposes)
df.show()
