import os
import json
import glob
import tqdm
import pandas as pd

files = glob.glob('data/*.zst')

files.sort()

for file_path in tqdm.tqdm(files):
    print(f'Processing file: {file_path}')

    data = pd.read_csv(file_path)

    # Convert DataFrame to list of dictionaries
    data_list = data.to_dict(orient='records')
    print(data_list[:2]) 
    break

