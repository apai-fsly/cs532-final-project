import json

class SparkConfig:
    """
    Configuration class for Apache Spark settings optimized for performance testing.
    
    This class manages Spark resource allocation and performance test parameters.
    Default values are optimized for a 16-core (20 logical core) Windows machine.
    
    Attributes:
        executor_memory (str): Memory per executor (e.g. "4g")
        executor_cores (int): Total cores for all Spark executors
        driver_memory (str): Memory allocation for Spark driver
        parallelism (int): Task parallelism (cores per executor)
        cores_to_test (list): Core values to use in performance tests
        memory_to_test (list): Memory values to use in performance tests
        parallelism_to_test (list): Parallelism values to use in performance tests
    """
    def __init__(self, 
                 executor_memory="8g",
                 executor_cores=16,
                 driver_memory="4g",
                 parallelism=4,
                 cores_to_test=None,
                 memory_to_test=None,
                 parallelism_to_test=None):
        
        # Core resources
        self.executor_memory = executor_memory  # Memory per executor
        self.executor_cores = executor_cores    # Total cores for Spark
        self.driver_memory = driver_memory      # Driver memory
        self.parallelism = parallelism          # Cores per executor (parallelism)
        
        # Performance test configurations
        # Default test values are defined to measure performance across different resource levels
        self.cores_to_test = cores_to_test or [4, 8, 12, 16]  # Testing different total core allocations
        self.memory_to_test = memory_to_test or ["4g", "8g", "12g", "16g"]  # Testing different memory allocations
        self.parallelism_to_test = parallelism_to_test or [1, 2, 4, 8]  # Testing different parallelism levels

    def __repr__(self):
        """String representation of the configuration for debugging"""
        return (f"SparkConfig("
                f"executor_memory='{self.executor_memory}', "
                f"executor_cores={self.executor_cores}, "
                f"driver_memory='{self.driver_memory}', "
                f"parallelism={self.parallelism}, "
                f"cores_to_test={self.cores_to_test}, "
                f"memory_to_test={self.memory_to_test}, "
                f"parallelism_to_test={self.parallelism_to_test})")

def load_config(path):
    """
    Load Spark configuration from JSON file with sensible defaults.
    
    Args:
        path (str): Path to the JSON configuration file
        
    Returns:
        SparkConfig: Configuration object with settings from file or defaults
    """
    try:
        with open(path, "r") as f:
            config_data = json.load(f)
            
        # Get performance test configurations if available
        perf_test = config_data.get("performance_test", {})
        
        return SparkConfig(
            executor_memory=config_data.get("executor_memory"),
            executor_cores=config_data.get("executor_cores"),
            driver_memory=config_data.get("driver_memory"),
            parallelism=config_data.get("parallelism"),
            cores_to_test=perf_test.get("cores_to_test"),
            memory_to_test=perf_test.get("memory_to_test"),
            parallelism_to_test=perf_test.get("parallelism_to_test")
        )
    except Exception as e:
        print(f"Error loading config from {path}: {e}")
        print("Using default configuration")
        return SparkConfig()