# Energy ETL Pipeline

>Automated energy data pipeline — Extract, Transform, Load, orchestrated with Airflow.

## What it does?
This pipeline extracts energy usuage data from source, transforms it into star schema and loads it into postgresql database - This task is automated by apache airflow

## Tech Stack
Python, 
Pannda, 
PostgreSQL, 
Apache Airflow, 
XCom for data exchange, 
Star Schema(Data Warehouse), 

## Pipeline
Extarct(CSV) >> Transform(Dim Table + Fact Table) >> Load(Sqlite)
