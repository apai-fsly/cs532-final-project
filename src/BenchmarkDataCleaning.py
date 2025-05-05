from pyspark.sql import SparkSession
import time
import os
import csv
from DataCleaning import clean_title_basics, clean_title_ratings, clean_title_akas, load_data
from SparkConfig import load_config
from CommonHelper import resolve_path, save_to_csv
from tabulate import tabulate
    
SparkConfigPath = resolve_path("./configurations/config.json")

def create_spark_session(cores=None, memory=None, parallelism=None):
    """Create a Spark session with configurable resources"""
    base_config = load_config(SparkConfigPath)

    builder = SparkSession.builder.appName("Title Akas Dataset Cleaning Performance Testing")
      
    # Set cores if provided, otherwise use config value
    if cores is not None:
        builder = builder.config("spark.cores.max", cores)
    else:
        builder = builder.config("spark.cores.max", base_config.executor_cores)
    
    # Set memory if provided, otherwise use config value
    if memory is not None:
        builder = builder.config("spark.executor.memory", memory)
        builder = builder.config("spark.driver.memory", base_config.driver_memory)
    else:
        builder = builder.config("spark.executor.memory", base_config.executor_memory)
        builder = builder.config("spark.driver.memory", base_config.driver_memory)
    
    # Set parallelism if provided, otherwise use config value
    if parallelism is not None:
        builder = builder.config("spark.executor.cores", parallelism)
    else:
        builder = builder.config("spark.executor.cores", base_config.parallelism)
    
    # Each task will use 1 CPU core
    builder = builder.config("spark.task.cpus", 1) 
    
    return builder.getOrCreate()

def test_cleaning_performance(test_param, param_value, dataset_path, test_id, sample_fraction=0.1):
    """Run a test focused on title.akas dataset cleaning performance with sampling"""
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
    
    print(f"\nTesting title.akas cleaning with {test_param}={param_value}")
    
    # Clear cache before starting the test
    spark.catalog.clearCache()
    
    # Load raw dataset
    print("  Loading title.akas dataset...")
    raw_df = load_data(spark, dataset_path)
    
    # Apply sampling to reduce dataset size
    print(f"  Sampling dataset ({sample_fraction*100:.1f}%)...")
    raw_df = raw_df.sample(fraction=sample_fraction, seed=42)
    
    
    # Start timing the cleaning operation
    print("===========Starting cleaning operation==========")
    start_time = time.time()
    
    # Run cleaning operation
    cleaned_df = clean_title_akas(raw_df)
    clean_count = cleaned_df.count()
    print(f"  Cleaned dataset has {clean_count} rows")

    
    # Complete timing
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Calculate throughput (processed rows per second)
    throughput = clean_count / execution_time if execution_time > 0 else 0
    


    print(f"  Time: {execution_time:.2f} seconds, Rows: {clean_count}")
   
    # Create result record
    result = {
        'test_id': test_id,
        'parameter': test_param,
        'value': param_value,
        'execution_time': execution_time,
        'rows': clean_count,
        'throughput': throughput,
        'sample_fraction': sample_fraction,

    }
        
    return result, spark

def main():
    print("Starting title.akas cleaning performance benchmarking...")
    
    # Define test ID (timestamp for unique identification)
    import datetime
    test_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Load base configuration
    base_config = load_config(SparkConfigPath)
    
    # Define path to title.akas dataset
    akas_path = resolve_path("./data/title.akas.tsv")

    # Set sampling fraction - adjust this based on your memory constraints
    # 0.01 = 1% of data, 0.2 = 20% of data
    sample_fraction = 0.2
    print(f"Using {sample_fraction*100:.1f}% sample of the dataset")
    
    # Define test parameters and values
    test_parameters = [
       {"name": "cores", "values": base_config.cores_to_test},
       {"name": "memory", "values": base_config.memory_to_test},
       {"name": "parallelism", "values": base_config.parallelism_to_test}
    ]
    
    # Store all results
    all_results = []
    
    # Check if dataset exists
    if not os.path.exists(akas_path):
        print(f"\nERROR: Dataset not found at {akas_path}")
        return
    
    # Run tests for each performance test parameter
    for param in test_parameters:
        param_name = param["name"]
        param_values = param["values"]
        
        print(f"\n{'*************'} TESTING {param_name.upper()} {'***************************'}")
        
        for value in param_values:
            print(f"\n============Testing {param_name}={value}=======")
            # Run cleaning performance test
            result, spark_session = test_cleaning_performance(
                param_name, value, akas_path, test_id,sample_fraction
            )
            all_results.append(result)
            
            # Stop Spark session after tests
            spark_session.stop()
            
            # Small delay between tests
            time.sleep(2)
    
    # Save all results to a CSV file
    csv_filename = f"akas_cleaning_results_{test_id}.csv"
    save_to_csv(all_results, csv_filename)
    
    # Print results in a formatted table
    if all_results:  
        print("\nTitle.Akas Cleaning Performance Results:")
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
            headers = ["Value", "Execution Time (s)", "Throughput (rows/s)", "Rows"]
            table_data = []
            
            for result in param_data:
                table_data.append([
                    result['value'],
                    f"{result['execution_time']:.2f}",
                    f"{result['throughput']:.2f}",
                    f"{result['rows']}",
                    
                ])
            
            # Print table
            print(f"\n****Title.Akas Cleaning Performance Results for {param.upper()}:****")
            print(tabulate(table_data, headers=headers, tablefmt="pretty"))

    print("\nPerformance testing completed!")
    print(f"Results saved to ./assets/results/{csv_filename}")


if __name__ == "__main__":
    main()