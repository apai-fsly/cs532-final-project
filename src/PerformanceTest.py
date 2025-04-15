from pyspark.sql import SparkSession
import time
import os
from DataCleaning import clean_title_basics, clean_title_ratings,load_data


#Create and return a Spark session with configurable resources
#Defaults are core = 4, memory = 4g, threads = 2
#We will vary one of these parameters at a time to see how it affects performance
def create_spark_session(cores, memory, threads):


    builder = SparkSession.builder.appName("Performance Testing")
      
    builder = builder.config("spark.cores.max", cores)
    builder = builder.config("spark.executor.memory", memory)
    builder = builder.config("spark.driver.memory", memory)
    builder = builder.config("spark.executor.cores", threads)
    builder = builder.config("spark.task.cpus", 1) #Each task will use 1 CPU core
    
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
def run_benchmarks(basics_path, ratings_path):
    
    # Configuration ranges to test
    cores_to_test = [1, 2, 4, 8]  # Adjust based on your system
    memory_to_test = ['1g', '2g', '4g', '8g']  # Memory configurations
    threads_to_test = [1, 2, 4]  # Threads per executor


    # Test different core configurations
    print("\n=== BENCHMARKING CPU CORES ===")
    for cores in cores_to_test:
        print(f"\nTesting with {cores} cores...")
        spark = create_spark_session(cores=cores, memory='4g', threads=2)
        
        # Load data
        basics_df, ratings_df = load_data(spark, basics_path, ratings_path)
        
        # Run benchmarks
        performance_test(
            clean_title_basics, basics_df, f"Cores={cores} - Clean Title Basics")
        
        performance_test(
            clean_title_ratings, ratings_df, f"Cores={cores} - Clean Title Ratings")
    
        spark.stop()
    
    # Test different memory configurations
    print("\n=== BENCHMARKING MEMORY ===")
    for memory in memory_to_test:
        print(f"\nTesting with {memory} memory...")
        spark = create_spark_session(cores=4, memory=memory, threads=2)
        
        # Load data
        basics_df, ratings_df = load_data(spark, basics_path, ratings_path)
        
        # Run benchmarks
        performance_test(
            clean_title_basics, basics_df, f"Memory={memory} - Clean Title Basics")
        
        performance_test(
            clean_title_ratings, ratings_df, f"Memory={memory} - Clean Title Ratings")
               
        spark.stop()
    
    # Test different thread configurations
    print("\n=== BENCHMARKING THREADS ===")
    for threads in threads_to_test:
        print(f"\nTesting with {threads} threads per executor...")
        spark = create_spark_session(cores=4, memory='4g', threads=threads)
        
        # Load data
        basics_df, ratings_df = load_data(spark, basics_path, ratings_path)
        
        # Run benchmarks
        performance_test(
            clean_title_basics, basics_df, f"Threads={threads} - Clean Title Basics")
        
        performance_test(
            clean_title_ratings, ratings_df, f"Threads={threads} - Clean Title Ratings")
        
        spark.stop()
    
    

def main():

    print("Starting performance benchmarking...")
    
    # Define paths to data files
    basics_path = "../data/title.basics.tsv"
    ratings_path = "../data/title.ratings.tsv"
    
    # Run all benchmarks
    run_benchmarks(basics_path, ratings_path)
    
    print("\nPerformance testing completed!")

if __name__ == "__main__":
    main()