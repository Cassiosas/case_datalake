import requests
import json
from datetime import datetime
import pandas as pd
import os

current_date = datetime.now()

extract_date = current_date.strftime("%Y-%m-%d")

def read_json_file(path):
    caminho = f"/home/cassio/case_datalake/datalake/bronze/{path}/{path}_{extract_date}.json"

    return pd.read_json(caminho)

def process_json_data(df, process_function):
    data = process_function(df)
    return pd.json_normalize(data)

def save_to_parquet(data, path):
    caminho = f"/home/cassio/case_datalake/datalake/silver/{path}/{path}_{extract_date}.parquet"

    if os.path.exists(caminho):
        df_existing = pd.read_parquet(caminho)
        df_combined = pd.concat([df_existing, data], ignore_index=True)
    else:
        data.to_parquet(caminho, index=False)

def process_data(df):
    breweries = []
    for item in df:
        for i in df[item]:
            breweries_data = { 
                'id': i['id'],
                'name': i['name'],
                'brewery_type': i['brewery_type'],
                'address': i['address_1'],
                'postal_code': i['postal_code'],
                'city': i['city'],
                'state': i['state'],
                'country': i['country'],
                'phone': i['phone'],
                'website': i['website_url']  
            }
            breweries.append(breweries_data)

    return breweries

def process_and_save(path, process_function):
    df = read_json_file(path)
    processed_data = process_json_data(df, process_function)
    save_to_parquet(processed_data, path)

process_and_save(path='breweries', process_function=process_data)