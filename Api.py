import pandas as pd
from sqlalchemy import create_engine
from fastapi import FastAPI
app = FastAPI()
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/energy_etl')

@app.get("/")
def home():
    return {"Message": "ENERGY ETL API is running"}

@app.get("/building")
def building():
    B_query = 'Select * from "Dim_Building"'
    df = pd.read_sql(B_query, engine)
    return df.to_dict(orient='records')

@app.get("/energy")
def energy():
    E_query = 'select * from "Fact_table"'
    df = pd.read_sql(E_query, engine)
    return df.to_dict(orient='records')

@app.get("/time")
def time():
    T_query = 'select * from "Dim_Time"'
    df = pd.read_sql(T_query, engine)
    return df.to_dict(orient='records')

