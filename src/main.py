from pyspark.sql import SparkSession

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
    .getOrCreate()

# Read data from the 'employees' table in MySQL
df = spark.read.jdbc(url=jdbc_url, table="employees", properties=connection_properties)

# Show the data from the database
df.show()
