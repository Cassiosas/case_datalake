import requests
import json
from datetime import datetime
import pandas as pd
import itertools


current_date = datetime.now()

extract_date = current_date.strftime("%Y-%m-%d")

def create_folder(path):
    bronze_output_directory = f"datalake/bronze/{path}/{path}_{extract_date}.json"
    bronze_output_directory_str = str(bronze_output_directory)

    return bronze_output_directory_str

def create_json(records, file_path):
    with open(file_path, "w") as output_file:
        json.dump(records, output_file, ensure_ascii=False)
        output_file.write("\n")

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)

# def read_json(path):

#     df = pd.read_json(f"/home/cassio/case_datalake/bronze/{path}/{path}_{extract_date}.json")

def request(method, endpoint: str) -> requests.Response:
        base_url = "https://api.openbrewerydb.org/"
        url = f"{base_url}/{endpoint}"

        result = []

        response = requests.request(method, url)

        data = response.json()

        result.append(data)

        # next_token = data.get("nextPageToken", "")

        # while next_token:
        #     params["pagaToken"] = next_token
        #     response = requests.request(method, url)
        #     data = response.json()
        #     result.append(data)
        #     next_token = data.get("nextPageToken", "")

        return result



def extract():
    response_breweries = request(
         method="GET",
         endpoint="breweries"
    )

    create_json(records=response_breweries, file_path=create_folder("breweries"))

#print(response.text)
extract()