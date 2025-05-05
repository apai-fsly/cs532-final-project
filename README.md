# CS532 Final Project
The Final Project for COMPSCI CS532 - Systems for Data Science.

This project showcases the complete workflow of handling large datasets, including ingestion, cleaning, transformation, and storage using modern data engineering tools. The pipeline involves downloading datasets from Kaggle, processing them with PySpark, and storing the cleaned data in a MySQL database hosted in a Docker container. Additionally, the project evaluates system performance by benchmarking batch ingestion under different configurations.

Link to Video: https://drive.google.com/file/d/1V4QmOgKDmmJ0hEERmfOov6aGFu1B2ywL/view?usp=sharing


Key components:
- **Data Download**: Automate dataset retrieval from Kaggle using the Kaggle API.
- **Data Cleaning**: Use PySpark to clean and preprocess raw datasets.
- **Data Storage**: Store cleaned data into a MySQL database using PySpark and JDBC.
- **Performance Benchmarking**: Analyze ingestion performance under varying configurations.

## Goals Checklist
- [x] Set up a MySQL database in a Docker container.
- [x] Test Python connectivity to the MySQL database.
- [x] Find and load the dataset with correct instructions.
- [x] Clean each relevant dataset and combine datasets using PySpark.
- [x] Ingest cleaned data into the MySQL database.
- [x] Benchmark batch ingestion performance with varying hardware configurations.
- [x] Plot performance insights comparing execution time vs hardware constraints.
- [x] Plot performance insights comparing rows processed per second.
- [ ] Configure the Read and Write Speeds of the Database to simulate the SSD and HDD.
- [ ] Measure the performance for performing queries on the data using Htop/lotop
- [ ] Plot a chart highlighting insights comparing query time vs. drive technology

## System Design

![System Design](./assets/System_Design.png "System Design Diagram")

## Prerequisite Software
1. Docker Daemon
2. Docker Compose
3. Python 3.x
4. PySpark
5. MySQL Connector for Python
6. Python-dotenv
7. Kaggle API
8. MatPlotLib



## Setup

### Install packages

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
    ```

4. **Install Python-dotenv**
    ```sh
    pip install python-dotenv
    ```

5. **Install Kaggle API**
    ```sh
    pip install kaggle
    ```
6. **Install MatPlotLib**
    ```sh
    pip install matplotlib



### Setup Environment File
Create a `.env` file in the [setup](./setup) directory and enter the following values. This file should **not** be committed to version control.

```sh
MYSQL_DATABASE=mydb
MYSQL_USER=myuser
MYSQL_PASSWORD=mypassword
MYSQL_ROOT_PASSWORD=rootpass
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DRIVER=com.mysql.cj.jdbc.Driver
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
 ```


### Getting Kaggle Credentials
1. Go to [Kaggle Account Settings](https://www.kaggle.com/settings/account).
2. Scroll down to the API section.
3. Click on `Create New API Token`. This will download a file called `kaggle.json`.
4. Open the `kaggle.json` file in a text editor. It will look like this:
```sh
{
  "username": "your_kaggle_username",
  "key": "your_kaggle_api_key"
}
 ```
5. Copy the `username` and `key` values and paste them into the `.env` file under `KAGGLE_USERNAME` and `KAGGLE_KEY`.


### Download Dataset from Kaggle

To download the dataset, follow these steps:

1. Ensure the `.env` file is correctly set up with your Kaggle credentials:

```sh
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

2. Run the `DataDownload.py` script:

```sh
cd src
python DataDownload.py
```

3. The dataset will be downloaded and unzipped into the [data](./data) directory.



## Testing Database Connectivity

1. Start the database container
From the [setup](./setup) directory, run :
```sh
`docker-compose up -d` 
```
This starts the database in detached mode.

2. Once the container is up and running, use the following query to create an `employees` table with some sample data:

```sh
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
```
3. Run `TestDatabaseSetup.py` to verify the data:

```sh
cd src
python TestDatabaseSetup.py
```

4. You should see the following output:

```sh
+---+-------+---+-----------+
| id|   name|age| department|
+---+-------+---+-----------+
|  1|  Alice| 30|Engineering|
|  2|    Bob| 25|  Marketing|
|  3|Charlie| 35|         HR|
|  4|  David| 40|Engineering|
|  5|    Eve| 28|    Finance|
+---+-------+---+-----------+
```

## Running the Application
 
1. Start the database container
From the [setup](./setup) directory, run 
```sh
`docker-compose up -d`
```
2. Switch to [src](./src) directory
```sh
cd src
```
3. Run `TestDatabaseSetup.py` to ensure the Database is set up correctly
```sh
python TestDatabaseSetup.py
```
4. Run `SparkPerformanceTest.py` to run benchmarking on data ingestion.
```sh
python SparkPerformanceTest.py
```
5. Run `TBD` to run benchmarking on query processing.

```sh
python TBD
```

