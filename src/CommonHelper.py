import csv
import os
from pyspark.sql import SparkSession



#Create a Spark session with specific configurations
def create_spark_session():
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName("Data Cleaning") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()




# Resolve the path to the project root directory
# This is useful for ensuring that file paths are correct regardless of where the script is run from.
# The project root is assumed to be the parent directory of the 'src' directory.
def resolve_path(relative_path):
    # Find the src directory from the current script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Determine project root (parent of src directory)
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    
    # Join with the requested relative path
    absolute_path = os.path.join(project_root, relative_path)
    
    return absolute_path


#Metod to anlayze null values in a DataFrame
# This method will analyze the null values in a DataFrame and print a simple table showing null/invalid counts for each column
# It will also calculate the percentage of null values in each column.
# This is useful for understanding the data quality and completeness of the dataset.
def analyze_nulls(df):
    """
    Analyzes a DataFrame and prints a simple table showing null/invalid counts for each column.
    
    Args:
        df: Spark DataFrame to analyze
    """
    # Get total number of rows
    total_rows = df.count()
    print(f"\nNull Value Analysis (Total Rows: {total_rows})")
    print(f"{'Column':<30} {'Null Count':<15} {'Percentage':<10}")
    print("-" * 55)
    
    # Check each column
    for column_name in df.columns:
        # Count nulls and "\\N" values
        null_count = df.filter(
            (df[column_name].isNull()) | 
            (df[column_name] == "\\N")
        ).count()
        
        # Calculate percentage
        percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
        
        # Print the result
        print(f"{column_name:<30} {null_count:<15,d} {percentage:>6.2f}%")


#Load cleaned datasets from parquet files
# This method will load pre-cleaned datasets from parquet files.
def load_cleaned_datasets(spark, data_dir=None):
    """
    Load pre-cleaned datasets from parquet files.
    
    Args:
        spark: Active SparkSession
        data_dir: Optional path to cleaned dataset directory
                 (defaults to ./data/cleaned_datasets)
                 
    Returns:
        Dictionary of cleaned DataFrames
    """
    import os
    
    if data_dir is None:
        data_dir = resolve_path("./data/cleaned_datasets")
    
    print(f"Loading cleaned datasets from {data_dir}...")
    
    cleaned_dfs = {}
    
    # Scan the directory to find all parquet datasets
    try:
        # List all subdirectories in the parquet directory
        subdirs = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
        
        if not subdirs:
            print(f"Warning: No dataset directories found in {data_dir}")
            return cleaned_dfs
        
        # Load each dataset found
        for dirname in subdirs:
            parquet_path = os.path.join(data_dir, dirname)
            try:
                print(f"Loading dataset from {parquet_path}...")
                # Use the directory name as the key
                cleaned_dfs[dirname] = spark.read.parquet(parquet_path)
                print(f"Successfully loaded {dirname}")
            except Exception as e:
                print(f"Error loading {dirname}: {e}")
                
    except Exception as e:
        print(f"Error accessing cleaned datasets directory: {e}")
    
    return cleaned_dfs

def verify_cleaned_datasets_exist(cleaned_data_dir):
    """
    Verify that all required cleaned datasets exist and return True/False
    
    This function checks if the cleaned datasets directory exists and contains
    all the required datasets for the performance tests.
    """
    if not os.path.exists(cleaned_data_dir):
        print(f"\nCleaned dataset directory not found: {cleaned_data_dir}")
        return False
        
    # List of required datasets
    required_datasets = [
        "title_basics", 
        "title_ratings", 
        "title_akas", 
        "title_crew", 
        "title_principals", 
        "name_basics", 
        "title_episode"
    ]
    
    # Check if each required dataset exists as a directory
    missing_datasets = []
    for dataset in required_datasets:
        dataset_path = os.path.join(cleaned_data_dir, dataset)
        if not os.path.isdir(dataset_path):
            missing_datasets.append(dataset)
            continue
            
        # Check if the directory contains parquet files
        parquet_files = [f for f in os.listdir(dataset_path) if f.endswith('.parquet')]
        if not parquet_files:
            missing_datasets.append(dataset)
    
    if missing_datasets:
        print(f"\nMissing or empty datasets: {', '.join(missing_datasets)}")
        return False
        
    print(f"\nAll required cleaned datasets found in {cleaned_data_dir}")
    return True

# Save test results to CSV
def save_to_csv(results, filename):
    """Save test results to a CSV file"""
    # Create results directory if it doesn't exist
    results_dir = resolve_path("./assets/results")
    os.makedirs(results_dir, exist_ok=True)
    
    # Full path to output file
    csv_file = os.path.join(results_dir, filename)
    
    # Use the first result's keys as fieldnames to preserve order
    if results:
        # Use the first result as the basis for column order
        fieldnames = list(results[0].keys())
        
        # Check for any missing fields in other results
        for result in results[1:]:
            for key in result.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
    else:
        fieldnames = []
    
    # Write to CSV
    with open(csv_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Results saved to CSV: {csv_file}")