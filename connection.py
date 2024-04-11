import os
import json
import psycopg2

def config(connection_db):
    path = os.getcwd()
    with open(path + '/config.json') as file: 
        conf = json.load(file)[connection_db]
    return conf