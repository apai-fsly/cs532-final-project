from pyspark.sql import SparkSession
import time
import os
import json
import csv
from DataCleaning import clean_datasets, create_master_table
from SparkConfig import load_config
from CommonHelper import resolve_path, load_cleaned_datasets, verify_cleaned_datasets_exist, save_to_csv
from dotenv import load_dotenv
from DataStorage import store_to_mysql
from tabulate import tabulate
    
SparkConfigPath = resolve_path("./configurations/config.json")
JDBCDriverPath = resolve_path("./jars/mysql-connector-j-9.2.0.jar")

def create_spark_session(cores=None, memory=None, parallelism=None):
    """Create a Spark session with configurable resources"""
    base_config = load_config(SparkConfigPath)

    builder = SparkSession.builder.appName("Full Pipeline Performance Testing")
      
    # Set cores if provided, otherwise use config value
    if cores is not None:
        builder = builder.config("spark.cores.max", cores)
    else:
        builder = builder.config("spark.cores.max", base_config.executor_cores)
    
    # Set memory if provided, otherwise use config value
    if memory is not None:
        builder = builder.config("spark.executor.memory", memory)
        builder = builder.config("spark.driver.memory", base_config.driver_memory) # Keep driver memory constant
    else:
        builder = builder.config("spark.executor.memory", base_config.executor_memory)
        builder = builder.config("spark.driver.memory", base_config.driver_memory)
    
    # Set parallelism if provided, otherwise use config value
    if parallelism is not None:
        builder = builder.config("spark.executor.cores", parallelism)
    else:
        builder = builder.config("spark.executor.cores", base_config.parallelism)
    
    # Add MySQL connector for database tests
    builder = builder.config("spark.jars", JDBCDriverPath)
    
    # Each task will use 1 CPU core
    builder = builder.config("spark.task.cpus", 1) 
    
    return builder.getOrCreate()


def test_full_pipeline(test_param, param_value, cleaned_data_dir, test_id):
    """Run a test for the full pipeline including transformation and DB ingestion"""
    
    # Set up the appropriate Spark configuration
    if test_param == "cores":
        spark = create_spark_session(cores=param_value)
    elif test_param == "memory":
        spark = create_spark_session(memory=param_value)
    elif test_param == "parallelism": 
        spark = create_spark_session(parallelism=param_value)
    
    print("ACTIVE CONFIGURATION:")
    for conf in sorted(spark.sparkContext.getConf().getAll()):
        print(f"  {conf[0]}: {conf[1]}")
    
    print(f"\nTesting full pipeline with {test_param}={param_value}")
    
    # Set up database connection
    env_path = resolve_path("./setup/.env")
    load_dotenv(dotenv_path=env_path) 
    
    jdbc_url = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DATABASE')}"
    connection_properties = {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    # Generate a unique table name
    table_name = f"full_pipeline_{test_param}_{param_value}_{test_id}".replace(".", "_")
    
    # Clear cache before starting the test
    spark.catalog.clearCache()
    
    print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
    
    # Load cleaned datasets
    print("  Loading cleaned datasets...")
    cleaned_dfs = load_cleaned_datasets(spark, cleaned_data_dir)
    
    # Start timing the full pipeline
    print("===========Starting full pipeline==========")
    start_time = time.time()
    
    # Step 1: Create master table
    print("  Step 1: Creating master table...")
    master_df = create_master_table(cleaned_dfs)
    
  
    # Get row count before database write
    row_count = master_df.count()
    
    # Step 2: Write to database
    print("  Step 2: Writing to database...")
    print(f"  Storing data into MySQL table: {table_name}")
    
    # Store to database
    success = store_to_mysql(
        master_df,
        table_name,
        jdbc_url,
        connection_properties
    )
    
    # Complete timing
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Calculate throughput
    throughput = row_count / execution_time if execution_time > 0 else 0
    
    print(f"  Time: {execution_time:.2f} seconds, Rows: {row_count}, Throughput: {throughput:.2f} rows/sec")
    
    # Create result record
    result = {
        'test_id': test_id,
        'parameter': test_param,
        'value': param_value,
        'execution_time': execution_time,
        'row_count': row_count,
        'throughput': throughput,
        'success': success
    }
    
    # Add table name if successful
    if success:
        result['table_name'] = table_name
        
    return result, spark


def main():
    print("Starting full pipeline performance benchmarking...")
    
    # Define test ID (timestamp for unique identification)
    import datetime
    test_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Load base configuration
    base_config = load_config(SparkConfigPath)
    
    # Define paths
    data_dir = resolve_path("./data")
    cleaned_data_dir = resolve_path("./data/cleaned_datasets")
    
    # Define test parameters and values
    test_parameters = [
       {"name": "cores", "values": base_config.cores_to_test},
        {"name": "memory", "values": base_config.memory_to_test},
        {"name": "parallelism", "values": base_config.parallelism_to_test}
    ]
    
    # Store all results
    all_results = []
    
    # Check if cleaned datasets exist, otherwise clean them
    if not verify_cleaned_datasets_exist(cleaned_data_dir):
        print("\nCleaning datasets (will be done only once)...")
        setup_spark = create_spark_session()
        clean_datasets(setup_spark, data_dir, analyze=False, save_to_parquet=True)
        setup_spark.stop()
        
        # Verify again that cleaning succeeded
        if not verify_cleaned_datasets_exist(cleaned_data_dir):
            print("\nERROR: Failed to create cleaned datasets. Exiting.")
            return
    
    # Run tests for each performance test parameter
    for param in test_parameters:
        param_name = param["name"]
        param_values = param["values"]
        
        print(f"\n{'*************'} TESTING {param_name.upper()} {'***************************'}")
        
        for value in param_values:
            print(f"\n============Testing {param_name}={value}=======")
            # Run full pipeline test
            result, spark_session = test_full_pipeline(
                param_name, value, cleaned_data_dir, test_id
            )
            all_results.append(result)
            
            # Stop Spark session after tests
            spark_session.stop()
            
            # Small delay between tests
            time.sleep(2)
    
    # Save all results to a CSV file
    csv_filename = f"full_pipeline_results_{test_id}.csv"
    save_to_csv(all_results, csv_filename)
    
    # Print results in a formatted table
    if all_results:  
        print("\nFull Pipeline Performance Results:")
        print(f"==========Test ID: {test_id}============")  
  
        # Group results by parameter
        param_results = {}
        for result in all_results:
            param = result['parameter']
            if param not in param_results:
                param_results[param] = []
            param_results[param].append(result)

        # Print table for each parameter
        for param, param_data in param_results.items():
            # Sort by value with special handling for memory values
            if param == "memory":
                # Extract numeric part from memory values (e.g., "4g" becomes 4)
                param_data.sort(key=lambda x: float(str(x['value']).replace('g', '').replace('m', '')))
            else:
                # For other parameters, try numeric sort first, fallback to string sort
                try:
                    param_data.sort(key=lambda x: float(str(x['value'])))
                except (ValueError, TypeError):
                    param_data.sort(key=lambda x: str(x['value']))
            
            # Format table data
            headers = ["Value", "Execution Time (s)", "Throughput (rows/s)", "Success"]
            table_data = []
            
            for result in param_data:
                table_data.append([
                    result['value'],
                    f"{result['execution_time']:.2f}",
                    f"{result['throughput']:.2f}",
                    "✓" if result.get('success', True) else "✗"
                ])
            
            # Print table
            print(f"\nFull Pipeline Performance Results for {param.upper()}:")
            print(tabulate(table_data, headers=headers, tablefmt="pretty"))

    print("\nPerformance testing completed!")
    print(f"Results saved to ./assets/results/{csv_filename}")


if __name__ == "__main__":
    main()