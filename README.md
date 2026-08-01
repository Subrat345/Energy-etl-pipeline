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

## Progress Log


