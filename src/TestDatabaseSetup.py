import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession


# Load environment variables from .env file
load_dotenv(dotenv_path="../db/.env") 

# Define the JDBC URL and connection properties
jdbc_url = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
connection_properties = {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "driver": os.getenv("MYSQL_DRIVER")
    }

# Initialize Spark session and specify the path to the JDBC driver
spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "../jars/mysql-connector-j-9.2.0.jar") \
    .getOrCreate()

# Read data from the 'employees' table in MySQL
df = spark.read.jdbc(url=jdbc_url, table="employees", properties=connection_properties)

# Show the data from the database
df.show()
