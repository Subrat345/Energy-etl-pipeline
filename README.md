# Energy ETL Pipeline

Automated energy data pipeline — Extract, Transform, Load, orchestrated with Airflow.

## What it does?
This pipeline extracts energy usuage data from source, transforms it into star schema and loads it into postgresql database - This task is automated by apache airflow

## Tech Stack
- Python, 
- Panda, 
- PostgreSQL, 
- Apache Airflow, 
- XCom for data exchange, 
- Star Schema(Data Warehouse), 
- Fast Api

## API Endpoints
- GET / - API Status
- GET /buildings - building types
- GET /energy - energy consumption data
- GET /time - time dimension data

## Pipeline
Extarct(CSV) >> Transform(Dim Table + Fact Table) >> Load(PostgreSQL)

## Project journey and challenges
**Issue 1 — Data sharing between Airflow tasks**
After transformation into star schema, the load stage 
couldn't access the transformed data. Each Airflow task 
runs in isolation, so calling transform() directly inside 
load() only works in plain Python, not in Airflow.
Fix: Used XCom — Airflow's built-in mechanism that allows 
different tasks within a DAG to share small pieces of data.

**Issue 2 — JSON Serialization**
After implementing XCom, Airflow couldn't store pandas 
DataFrames because XCom stores data as text/JSON in a 
database — not Python objects.
Fix: Used to_json() to convert DataFrames to JSON strings 
before pushing to XCom.

**Issue 3 — StringIO**
After pulling JSON string from XCom in load(), 
pd.read_json() treated it as a file path instead of 
actual data.
Fix: Wrapped the string with StringIO to tell pandas 
it is actual data, not a file path — read directly 
from memory.


