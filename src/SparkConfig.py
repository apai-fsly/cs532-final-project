import json

class BatchIngestionConfig:
    def __init__(self, 
                 executor_memory="2g",
                 executor_cores=2,
                 driver_memory="1g",
                 parallelism=4): 
                #  shuffle_partitions=None,
                #  memory_fraction="0.6",
                #  storage_fraction="0.5",
                #  network_timeout="120s",
                #  sql_adaptive_enabled="true",
                #  temp_dir="/tmp/spark-temp"):
        
        # Core resources
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.driver_memory = driver_memory
        self.parallelism = parallelism
        
        # Performance tuning
        # self.shuffle_partitions = shuffle_partitions or parallelism * 2
        # self.memory_fraction = memory_fraction
        # self.storage_fraction = storage_fraction
        # self.sql_adaptive_enabled = sql_adaptive_enabled
        
        # System settings
        # self.network_timeout = network_timeout
        # self.temp_dir = temp_dir

    def __repr__(self):
        return (f"BatchIngestionConfig(executor_memory='{self.executor_memory}', "
                f"executor_cores={self.executor_cores}, "
                f"driver_memory='{self.driver_memory}', "
                f"parallelism={self.parallelism}, ")

def load_config(path):
    """Load configuration from JSON with sensible defaults"""
    with open(path, "r") as f:
        config_data = json.load(f)
    
    return BatchIngestionConfig(
        executor_memory=config_data.get("executor_memory"),
        executor_cores=config_data.get("executor_cores"),
        driver_memory=config_data.get("driver_memory"),
        parallelism=config_data.get("parallelism"),
        # shuffle_partitions=config_data.get("shuffle_partitions"),
        # memory_fraction=config_data.get("memory_fraction"),
        # storage_fraction=config_data.get("storage_fraction"),
        # network_timeout=config_data.get("network_timeout"),
        # sql_adaptive_enabled=config_data.get("sql_adaptive_enabled"),
        # temp_dir=config_data.get("temp_dir")
    )