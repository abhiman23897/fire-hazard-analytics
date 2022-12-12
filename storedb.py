from pymongo import MongoClient
from datetime import datetime

import time

IMG_PATH = "results/"

def find_time():
    curr_time = time.time()
    date_time = datetime.fromtimestamp(curr_time)
    str_time = date_time.strftime("%m-%d-%Y%H-%M-%S")
    return str_time


def save_in_db(figure_name, pipeline_name):
    CONNECTION_STRING = "mongodb://localhost:27017"
 
    client = MongoClient(CONNECTION_STRING)
 
    dbname = client['fireHazardsAnalyticsDb']

    collection_name = dbname["PipelineOutputs"]

    timestamp = find_time()
    path = f"{IMG_PATH}{figure_name}-{timestamp}"
    collection_name.insert_one({
        "path": path, 
        "pipeline": pipeline_name,
        "timestamp": timestamp
        })