import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from CommonHelper import resolve_path

envpath = resolve_path("./setup/.env")
jdbsc_driver_path = resolve_path("./jars/mysql-connector-j-9.2.0.jar")  

# Load environment variables from .env file

load_dotenv(dotenv_path=envpath) 


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
    .config("spark.jars", jdbsc_driver_path) \
    .getOrCreate()

# Set log level to ERROR to disable warnings
spark.sparkContext.setLogLevel("ERROR")
    
# Read data from the 'employees' table in MySQL
df = spark.read.jdbc(url=jdbc_url, table="employees", properties=connection_properties)

# Show the data from the database
df.show()
