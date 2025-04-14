import time
from pyspark.sql import SparkSession
from SparkConfig import load_config


CONFIG_PATH = "/Users/ashwinpai/src/ash/cs532-final-project/config.json"

config = load_config(path=CONFIG_PATH)

print(config)

# Define the JDBC URL and connection properties
jdbc_url = "jdbc:mysql://localhost:3306/mydb"
connection_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Initialize Spark session and specify the path to the JDBC driver
spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "./jars/mysql-connector-j-9.2.0.jar") \
    .config("spark.executor.memory", config.executor_memory) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

# Read data from the 'employees' table in MySQL
df = spark.read.jdbc(url=jdbc_url, table="employees", properties=connection_properties)

# Show the data from the database
df.show()

# set this value in case you dont want to close the spark session
time.sleep(100000)

