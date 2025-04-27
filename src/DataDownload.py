import os
from dotenv import load_dotenv
from CommonHelper import resolve_path

# Make sure that the Kaggle API is installed and configured.
# Add Kaggle Credentials to your environment variables
# Get your Kaggle API credentials from https://www.kaggle.com/settings/account

# Load environment variables from .env file
env_path = resolve_path("setup/.env")
load_dotenv(dotenv_path=env_path)
print(f"Loaded environment from: {env_path}")

# Set Kaggle credentials from environment variables
os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME")
os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY")

# Verify credentials are loaded
if not os.environ.get("KAGGLE_USERNAME") or not os.environ.get("KAGGLE_KEY"):
    print("WARNING: Kaggle credentials not found. Please check your .env file.")

# Kaggle needs the credentials to be set before it's imported
from kaggle import KaggleApi

def main():
    try:
        # Initialize Kaggle API
        print("Initializing Kaggle API...")
        api = KaggleApi()
        api.authenticate()
        
        # Set the data download path
        data_path = resolve_path("data")
        
        
        # Download and unzip the dataset
        dataset = "rahulsaranm/imdb-dataset"
        print(f"Downloading dataset '{dataset}' to {data_path}...")
        api.dataset_download_files(dataset, path=data_path, quiet=False, unzip=True)
        
        print(f"Dataset successfully downloaded to {data_path}")
    except Exception as e:
        print(f"Error downloading dataset: {e}")

if __name__ == "__main__":
    main()