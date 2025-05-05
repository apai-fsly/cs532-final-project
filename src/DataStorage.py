import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json
from DataCleaning import clean_datasets, create_master_table  # Import cleaning functions
from SparkConfig import load_config
from CommonHelper import load_cleaned_datasets, resolve_path

SparkConfigPath= resolve_path("./configurations/config.json")
JDBCDriverPath = resolve_path("./jars/mysql-connector-j-9.2.0.jar")

config = load_config(path=SparkConfigPath)

def create_spark_session(config):
    """Create and return a Spark session."""
    print(config)
    return SparkSession.builder \
        .appName("Data Storage") \
        .config("spark.executor.memory", config.executor_memory) \
        .config("spark.driver.memory", config.driver_memory) \
        .config("spark.jars", JDBCDriverPath) \
        .getOrCreate()


#Method to store the cleaned DataFrame into a MySQL database using JDBC
#This is a simple method to verify the data storage process that will be used later in the pipeline
#We will use the JDBC driver to connect to MySQL and store the DataFrame
def store_to_mysql(df: DataFrame, table_name: str, jdbc_url: str, connection_properties: dict):
    """
    Store the PySpark DataFrame into a MySQL database table using JDBC.
    
    Args:
        df (DataFrame): The PySpark DataFrame to store
        table_name (str): Name of the MySQL table
        jdbc_url (str): JDBC URL for MySQL connection
        connection_properties (dict): Connection properties including user, password, and driver
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        print(f"Storing data into MySQL table: {table_name}")

        # Convert arrays to strings before storing
        mysql_df = prepare_dataframe_for_mysql(df)

        # Write the DataFrame to MySQL
        mysql_df.write.jdbc(
            url=jdbc_url, 
            table=table_name, 
            mode="overwrite", 
            properties=connection_properties
        )
        
        print(f"Data successfully stored in table: {table_name}")
        return True
    
    except Exception as e:
        print(f"Error while storing data to MySQL: {e}")
        return False


# Method to prepare DataFrame for MySQL storage
# This method will convert array columns to strings for MySQL compatibility
def prepare_dataframe_for_mysql(df):
    """Convert array columns to strings for MySQL compatibility"""
    # Get the schema
    schema = df.schema
    
    # Identify array columns
    array_columns = [field.name for field in schema.fields 
                    if str(field.dataType).startswith("ArrayType")]
    
    if array_columns:
        print(f"Converting array columns to strings: {array_columns}")
        
        # Create a new DataFrame with array columns converted to strings
        modified_df = df
        
        for array_col in array_columns:
            # Option 1: Convert arrays to JSON strings
            modified_df = modified_df.withColumn(
                array_col, 
                to_json(col(array_col))
            )           
        
        return modified_df
    else:
        return df

def main():

     # Load environment variables from .env file
    env_path = resolve_path("./setup/.env")
    load_dotenv(dotenv_path=env_path) 

    # Fetch MySQL connection properties from environment variables
    jdbc_url = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
    connection_properties = {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "driver": os.getenv("MYSQL_DRIVER")
    }

    # Create Spark session
    spark = create_spark_session(config)

    # Define data directory
    data_dir = resolve_path("./data")
    cleaned_data_dir = resolve_path("./data/cleaned_datasets")

  # Check if cleaned datasets exist, otherwise clean the datasets
    if not os.path.exists(cleaned_data_dir) or len(os.listdir(cleaned_data_dir)) == 0:
        print("\nCleaned datasets not found. Cleaning all IMDb datasets...")
        cleaned_dfs = clean_datasets(spark, data_dir, analyze=False, save_to_parquet=True)
    else:
        print("\nLoading pre-cleaned datasets...")
        cleaned_dfs = load_cleaned_datasets(spark)


    # Create master integrated table
    print("\nCreating master integrated table...")
    master_df = create_master_table(cleaned_dfs)

    
    # Show sample from master table
    print("\nSample rows from master table:")
    master_df.show(5, truncate=False)



    # Store the cleaned DataFrames into the MySQL database
    print("Storing cleaned data into MySQL...")
    store_to_mysql(master_df, "IMDBMasterTable", jdbc_url, connection_properties)



    #Fetch top 10 records from the MySQL table to verify the data
    print("Fetching top 10 records from TitleBasics...")
    fetched_basics = spark.read.jdbc(url=jdbc_url, table="IMDBMasterTable", properties=connection_properties)
    fetched_basics.show(10)


    spark.stop()

if __name__ == "__main__":
    main()