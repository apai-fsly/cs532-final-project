import json

# https://spark.apache.org/docs/latest/configuration.html

class BatchIngestionConfig: 
    def __init__(self, executor_memory, executor_cores, driver_memory, parallelism):
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.driver_memory = driver_memory
        self.parallelism = parallelism

    def __repr__(self):
        return f"BatchIngestionConfig(executor_memory='{self.executor_memory}', " \
               f"executor_cores={self.executor_cores}, " \
               f"driver_memory='{self.driver_memory}', " \
               f"parallelism={self.parallelism})"


def load_config(path): 
    # Load resource configuration from JSON
    with open(path, "r") as f:
        config = json.load(f)

    return BatchIngestionConfig(
        executor_memory=config.get("executor_memory", "2g"),
        executor_cores=config.get("executor_cores", 2),
        driver_memory=config.get("driver_memory", "1g"),
        parallelism=config.get("parallelism", 4)
    )
