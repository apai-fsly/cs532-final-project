# CS532-Final-project
The Final Project for CS532 - System for Data Science

## Milestone Checklist
- [x] Set up a MySQL database in a Docker container.
- [x] Test Python connectivity to the MySQL database.
- [ ] Clean each relevant dataset and combine datasets using PySpark.
- [ ] Ingest cleaned data into the MySQL database.
- [ ] Benchmark batch ingestion performance with varying hardware configurations.
- [ ] Plot performance insights comparing rows processed per second.

## Prerequisite Software
1. Docker Daemon
2. Docker Compose
3. Python 3.x
4. PySpark
5. MySQL Connector for Python
6. Python-dotenv

## Setup

# Setting Up the Environment

1. **Update package lists**
    ```sh
    sudo apt update
    ```

2. **Install OpenJDK 8**
    ```sh
    sudo apt install openjdk-8-jdk -y
    ```

3. **Install PySpark**
    ```sh
    pip install pyspark

4. **Install Python-dotenv**
    ```sh
    pip install python-dotenv


## Running the Application
1. Start the database container
From the /db directory run `docker-compose up -d` to start the database in detached mode
2. Switch to /src directory
3. Run main.py
4. Run DataCleaning.py
5. Run DataStorage.py


## Database Credentials
Create .env file in the db directory and enter the following values. This file should not be committed when pushing forward changes.

MYSQL_DATABASE=mydb
MYSQL_USER=myuser
MYSQL_PASSWORD=mypassword
MYSQL_ROOT_PASSWORD=rootpass
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DRIVER=com.mysql.cj.jdbc.Driver

## Testing Database Connectivity
Once the container is up and running you can use the following query to create a Employees Table with some sample data

CREATE TABLE IF NOT EXISTS employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    department VARCHAR(100)
);

INSERT INTO employees (id, name, age, department)
VALUES
(1, 'Alice', 30, 'Engineering'),
(2, 'Bob', 25, 'Marketing'),
(3, 'Charlie', 35, 'HR'),
(4, 'David', 40, 'Engineering'),
(5, 'Eve', 28, 'Finance');

You should see the following output
ashwinpai@XKX9DY07Q1 cs532-final-project % /usr/bin/python3 /Users/ashwinpai/src/ash/cs532-f
inal-project/src/main.py
25/04/09 18:31:24 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
+---+-------+---+-----------+
| id|   name|age| department|
+---+-------+---+-----------+
|  1|  Alice| 30|Engineering|
|  2|    Bob| 25|  Marketing|
|  3|Charlie| 35|         HR|
|  4|  David| 40|Engineering|
|  5|    Eve| 28|    Finance|
+---+-------+---+-----------+

ashwinpai@XKX9DY07Q1 cs532-final-project % 