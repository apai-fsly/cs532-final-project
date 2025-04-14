import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from DataCleaning import clean_title_basics, clean_title_ratings  # Import cleaning functions
from SparkConfig import load_config

config = load_config(path=CONFIG_PATH)

def create_spark_session(config):
    """Create and return a Spark session."""
    print(config)
    return SparkSession.builder \
        .appName("Data Storage") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars", "../jars/mysql-connector-j-9.2.0.jar") \
        .getOrCreate()

def store_to_mysql(df: DataFrame, table_name: str, jdbc_url: str, connection_properties: dict):
    """Store the PySpark DataFrame into a MySQL database table using JDBC."""
    start_time = time.time()
    try:
        print(f"Storing data into MySQL table: {table_name}")
        df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)
        end_time = time.time() - start_time
        print(f"Data successfully stored in table: {table_name} in {end_time} seconds")
    except Exception as e:
        print(f"Error while storing data to MySQL: {e}")

def main():

     # Load environment variables from .env file
    load_dotenv(dotenv_path="../setup/.env") 

    # Fetch MySQL connection properties from environment variables
    jdbc_url = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
    connection_properties = {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "driver": os.getenv("MYSQL_DRIVER")
    }

    # Create Spark session
    spark = create_spark_session()


    # Load raw datasets
    basics_path = "../data/title.basics.tsv"
    ratings_path = "../data/title.ratings.tsv"

    print("Loading raw datasets...")
    raw_basics = spark.read.option("sep", "\t") \
                           .option("header", "true") \
                           .option("nullValue", "\\N") \
                           .csv(basics_path)

    raw_ratings = spark.read.option("sep", "\t") \
                            .option("header", "true") \
                            .option("nullValue", "\\N") \
                            .csv(ratings_path)

    # Clean datasets using functions from DataCleaning.py
    print("Cleaning datasets...")
    start_time = time.time()
    cleaned_basics = clean_title_basics(raw_basics)
    cleaned_ratings = clean_title_ratings(raw_ratings)
    total_time = start_time - time.now()
    print(str(total_time) + " seconds for cleaning the data")


    # Store the cleaned DataFrames into the MySQL database
    print("Storing cleaned data into MySQL...")
    store_to_mysql(cleaned_basics, "TitleBasics", jdbc_url, connection_properties)
    store_to_mysql(cleaned_ratings, "TitleRatings", jdbc_url, connection_properties)


    #Fetch top 10 records from the MySQL table to verify the data
    print("Fetching top 10 records from TitleBasics...")
    fetched_basics = spark.read.jdbc(url=jdbc_url, table="TitleBasics", properties=connection_properties)
    fetched_basics.show(10)

    print("Fetching top 10 records from TitleRatings...")
    fetched_ratings = spark.read.jdbc(url=jdbc_url, table="TitleRatings", properties=connection_properties)
    fetched_ratings.show(10)

    spark.stop()

if __name__ == "__main__":
    main()