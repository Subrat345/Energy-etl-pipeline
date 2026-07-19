from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
#from airflow.decorators import dag, task
import pandas as pd
import sqlite3
import os


#@task
def extract():
    csv_path = os.path.join(os.path.dirname(__file__), "test_energy_data.csv")
    df = pd.read_csv(csv_path)
    print(df.columns)

#@task
#Finding the Unique values
def transform():
    csv_path = os.path.join(os.path.dirname(__file__), "test_energy_data.csv")
    df = pd.read_csv(csv_path)
    #Build_transform
    build_df = df[['Building Type']].drop_duplicates().reset_index(drop=True)
    build_df.insert(0, 'Building_ID', range(1, len(build_df) + 1))
    #Time_Transform
    time_df = df[['Day of Week']].drop_duplicates().reset_index(drop=True)
    time_df.insert(0, 'Time_ID', range(1, len(time_df) + 1))
    #Temp_Transform
    env_df = df[['Average Temperature']].copy()
    env_df['Temp Category'] = pd.cut(
    df['Average Temperature'], 
    bins = [0, 15, 25, 100], 
    labels = ['cold', 'moderate', 'hot']
    )
    env_df = env_df[['Temp Category']].drop_duplicates().reset_index(drop=True)
    env_df.insert(0, 'Environment_Id', range(1, len(env_df) + 1))
    #Merge and transform fact_table
    fact_df = df.merge(build_df, on='Building Type', how='left')
    print("Build_df Merged")
    fact_df = fact_df.merge(time_df, on='Day of Week', how='left')
    print("Time_df Merged")
    fact_df['Temp Category'] = pd.cut(
        df['Average Temperature'],
        bins=[0, 15, 25, 100],
        labels=['Cold', 'Moderate', 'Hot']
    )
    fact_df = fact_df.merge(env_df, on='Temp Category', how='left')
    print("Env_df Merged")

    return build_df, env_df, time_df, fact_df

#@task
def load():
    build_df, env_df, time_df, fact_df = transform()
    conn = sqlite3.connect('FinalDB.db')
    build_df.to_sql('Dim_Building', conn, if_exists='replace', index=False)
    env_df.to_sql('Dim_Environment', conn, if_exists='replace', index=False)
    time_df.to_sql('Dim_Time', conn, if_exists='replace', index=False)
    fact_df.to_sql('Fact_table', conn, if_exists='replace', index=False)
    conn.close()
    print("Load Successful")

#defining DAG args
default_args = {
    "owner":"airflow",
    "start_date":days_ago(0),
    "email":["sonusubrat34@gmail.com"],
    "email_on_failure":True,
    "email_on_retry":True,
    "retry_delay":timedelta(minutes=5),
    "retries":1,
}

#def energy_etl():
    #data = extract()
    #transformed = transform(data)
    #load(transformed)
#energy_etl()
#dag = dag

#Defining DAG
dag = DAG(
    "Energy_ETL_pipeline",
    schedule_interval=timedelta(days=1),
    default_args=default_args
)

#Task-1 Extract
execute_extract = PythonOperator(
    task_id = "Extract_data_from_csv",
    python_callable=extract,
    dag=dag
)

#Task-2 Transform
#execute_transform = PythonOperator(
    #task_id = "transform_data",
    #python_callable=transform,
    #dag = dag
#)

#Task-3 Load
execute_load = PythonOperator(
    task_id = "Load_data",
    python_callable=load,
    dag = dag
)

#Task pipeline
execute_extract >> execute_load