import os
from dotenv import load_dotenv


# Make sure that the Kaggle API is installed and configured.
# Add Kaggle Credentials to your environment variables
# Get your Kaggle API credentials from https://www.kaggle.com/settings/account



# Load environment variables from .env file
load_dotenv(dotenv_path="../setup/.env")

# Set Kaggle credentials from environment variables
os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME")
os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY")


#Kaggle needs the credentials to be set before it's imported
from kaggle import KaggleApi


# Initialize Kaggle API
api = KaggleApi()
api.authenticate()

# Download and unzip the dataset
dataset = "rahulsaranm/imdb-dataset"
api.dataset_download_files(dataset, path='../data',quiet=False, unzip=True)
