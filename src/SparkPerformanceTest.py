from pyspark.sql import SparkSession
import time
import os
from DataCleaning import clean_title_basics, clean_title_ratings,load_data
from SparkConfig import load_config
from CommonHelper import resolve_path
from ChartPlot import create_performance_chart, ensure_output_dir
    
SparkConfigPath =resolve_path("./configurations/config.json")

#Create and return a Spark session with configurable resources
#We will vary one of these parameters at a time to see how it affects performance
def create_spark_session(cores=None, memory=None, threads=None):

    # Load base configuration
    
    base_config = load_config(SparkConfigPath)

    #Create a Spark session with the specified configurations
    builder = SparkSession.builder.appName("Performance Testing")
      
    # Set cores if provided, otherwise use config value
    if cores is not None:
        builder = builder.config("spark.cores.max", cores)
    else:
        builder = builder.config("spark.cores.max", base_config.executor_cores)
    
    # Set memory if provided, otherwise use config value
    if memory is not None:
        builder = builder.config("spark.executor.memory", memory)
        builder = builder.config("spark.driver.memory", memory)
    else:
        builder = builder.config("spark.executor.memory", base_config.executor_memory)
        builder = builder.config("spark.driver.memory", base_config.driver_memory)
    
    # Set threads if provided, otherwise use config value
    if threads is not None:
        builder = builder.config("spark.executor.cores", threads)
    else:
        builder = builder.config("spark.executor.cores", base_config.parallelism)
    
    #Each task will use 1 CPU core
    builder = builder.config("spark.task.cpus", 1) 
    
    return builder.getOrCreate()


#Benchmark the execution time of a specific operation
#This method takes a function to run, the data to process, and the name of the test we are doing

def performance_test(function_to_run, data_df, test_name):

    start_time = time.time()
    result_df = function_to_run(data_df)
    
    # Force full evaluation by collecting results
    collected_result = result_df.collect()
    row_count = len(collected_result)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"{test_name} - Time: {execution_time:.2f} seconds, Rows: {row_count}")
    return execution_time, row_count


# Run the benchmarks with different configurations
def run_benchmarks(function_to_run,datasetpath):
    
    # Load base configuration to see what ranges we should test
    base_config = load_config(SparkConfigPath)

    # Use test ranges from config
    cores_to_test = base_config.cores_to_test
    memory_to_test = base_config.memory_to_test
    threads_to_test = base_config.threads_to_test
    
    print(f"Testing cores: {cores_to_test}")
    print(f"Testing memory: {memory_to_test}")
    print(f"Testing threads: {threads_to_test}")

    # Initialize results list to collect all benchmark data
    results = []

    # Test different core configurations
    print("\n=== BENCHMARKING CPU CORES ===")
    for cores in cores_to_test:
        print(f"\nTesting with {cores} cores...")
        spark = create_spark_session(cores=cores, memory=base_config.executor_memory, threads=2)
        
        # Load data
        dataset_df= load_data(spark,datasetpath)
        
       # Run benchmarks and capture results
        execution_time, row_count = performance_test(
            function_to_run, dataset_df, f"Cores={cores} - {function_to_run.__name__}")    
        
        # Store results
        results.append({
            'parameter': 'cores',
            'value': cores,
            'execution_time': execution_time,
            'row_count': row_count,
            'function_name': function_to_run.__name__
        })
        
        
        spark.stop()
    
    # Test different memory configurations
    print("\n=== BENCHMARKING MEMORY ===")
    for memory in memory_to_test:
        print(f"\nTesting with {memory} memory...")
        spark = create_spark_session(cores=base_config.executor_cores, memory=memory, threads=2)   
        
        # Load data
        dataset_df = load_data(spark, datasetpath)
        
        # Run benchmarks and capture results
        execution_time, row_count = performance_test(
            function_to_run, dataset_df, f"Memory={memory} - {function_to_run.__name__}")
        
        # Store results
        results.append({
            'parameter': 'memory',
            'value': memory,
            'execution_time': execution_time,
            'row_count': row_count,
            'function_name': function_to_run.__name__
        })
               
        spark.stop()
    
    # Test different thread configurations
    print("\n=== BENCHMARKING THREADS ===")
    for threads in threads_to_test:
        print(f"\nTesting with {threads} threads per executor...")
        spark = create_spark_session(cores=base_config.executor_cores, memory=base_config.executor_memory, threads=threads)
        
        # Load data
        dataset_df= load_data(spark,datasetpath)
        
        # Run benchmarks and capture results
        execution_time, row_count = performance_test(
            function_to_run, dataset_df, f"Threads={threads} - {function_to_run.__name__}")
        
        # Store results
        results.append({
            'parameter': 'threads',
            'value': threads,
            'execution_time': execution_time,
            'row_count': row_count,
            'function_name': function_to_run.__name__
        })
        
        
        spark.stop()

    # Calculate throughput for all results 
    for result in results:
        # Calculate rows processed per second (throughput)
        if result['execution_time'] > 0:  # Avoid division by zero
            result['throughput'] = result['row_count'] / result['execution_time']
        else:
            result['throughput'] = 0    
    return results    
    

def main():

    print("Starting performance benchmarking...")
    
    # Define paths to data files
    basics_path = resolve_path("./data/title.basics.tsv")
    ratings_path =resolve_path("./data/title.ratings.tsv")
    
    # Run benchmarks 
    print("\nRunning benchmarks for title.basics dataset...")
    basics_results = run_benchmarks(clean_title_basics, basics_path)
    
    # Create output directory
    output_dir = ensure_output_dir()
    
    # Generate performance charts for each parameter
    for parameter in ['cores', 'memory', 'threads']:
        # Generate execution time chart
        create_performance_chart(
            results=basics_results,
            parameter=parameter,
            metrics=['execution_time'],
            labels=['Execution Time'],
            title=f'Title Basics Performance by {parameter.capitalize()}',
            output_dir=output_dir,
            chart_type='execution_time',
            y_label='Time (seconds)',
        )
    
         # Generate throughput chart (higher is better)
        create_performance_chart(
            results=basics_results,
            parameter=parameter,
            metrics=['throughput'],
            labels=['Throughput'],
            title=f'Title Basics Throughput by {parameter.capitalize()}',
            y_label='Rows Processed per Second',
            output_dir=output_dir,
            chart_type='throughput',
            
        )
    
    print(f"Charts saved to {output_dir}")
    print("\nPerformance testing completed!")


    print("\nPerformance testing completed!")

if __name__ == "__main__":
    main()