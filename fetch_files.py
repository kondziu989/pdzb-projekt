import zipfile
import os
from kaggle.api.kaggle_api_extended import KaggleApi

DIRNAME = '/home/cloudera/Desktop/pdzb/files'

def fetch_files():
    # raise Exception('Test exception')
    api = KaggleApi()
    api.authenticate()
    print('Downloading...')
    # kaggle datasets download -d rohanrao/formula-1-world-championship-1950-2020
    api.dataset_download_files("rohanrao/formula-1-world-championship-1950-2020", path='/home/cloudera/Desktop/pdzb')

    print('Unzipping...')
    with zipfile.ZipFile('/home/cloudera/Desktop/pdzb/formula-1-world-championship-1950-2020.zip', 'r') as zip_ref:
        zip_ref.extractall(DIRNAME)
    print('Removing file...')
    os.remove("/home/cloudera/Desktop/pdzb/formula-1-world-championship-1950-2020.zip")
    print('Done.')


