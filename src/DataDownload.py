import os
from kaggle.api.kaggle_api_extended import KaggleApi

# make sure that your kaggle.json file is in $pwd/.kaggle directory as kaggle.json
# https://www.kaggle.com/docs/api#getting-started-installation-&-authentication 
# explain how to do this

api = KaggleApi()
api.authenticate()

dataset = "rahulsaranm/imdb-dataset"
api.dataset_download_files(dataset, path='./data', unzip=True)
