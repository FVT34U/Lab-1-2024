from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime
import os
from pathlib import Path

import pandas as pd

from elasticsearch import Elasticsearch

data_dir_path = os.path.join(os.path.dirname(__file__), '..', 'data')

def csv_read():
    df = pd.DataFrame()
    file_to_ignore = ""

    for file in Path(data_dir_path).rglob("*.csv"):
        df = pd.read_csv(str(file))
        file_to_ignore = str(file)
        break

    for file in Path(data_dir_path).rglob("*.csv"):
        if file_to_ignore == str(file):
            continue

        df = pd.concat([df, pd.read_csv(str(file))], axis=0, ignore_index=True)

    Variable.set("df", df.to_json())
        

def df_prep():
    try:
        df = pd.read_json(Variable.get("df"))
    except:
        print("DataFrame Error in df_prep(): read from json failed")

    df = df.dropna(subset=['designation', 'region_1'])

    df['price'] = df['price'].fillna(0.0)

    Variable.set("df", df.to_json())

def save_csv():
    try:
        df = pd.read_json(Variable.get("df"))
    except:
        print("DataFrame Error in save_csv(): read from json failed")

    filepath = os.path.join(data_dir_path, "result.csv")

    try:
        df.to_csv(filepath, index=False)
    except:
        print(f"Save to .csv Error in save_csv(): save to {filepath} failed")

def send_to_es():
    try:
        df = pd.read_json(Variable.get("df"))
    except:
        print("DataFrame Error in send_to_es(): read from json failed")
    
    es = Elasticsearch([{'host': 'elasticsearch-kibana', 'port': 9200}])

    for _, row in df.iterrows():
        response = es.index(index="airflow_index", body=row.to_json())
        
        if response['result'] == 'created':
            print("Data successfully sended to Elasticsearch")
        else:
            print(f"Sending Error in send_to_es(): {response}")

with DAG(
    dag_id='dataflow_preparation',
    schedule_interval=None,
    start_date=datetime.now(),
    catchup=False
) as dag:
    
    task1 = PythonOperator(
        task_id='csv_read',
        python_callable=csv_read
    )

    task2 = PythonOperator(
        task_id='df_prep',
        python_callable=df_prep
    )

    task3 = PythonOperator(
        task_id='save_csv',
        python_callable=save_csv
    )

    task4 = PythonOperator(
        task_id='send_to_es',
        python_callable=send_to_es
    )

    task1 >> task2 >> [task3, task4]
