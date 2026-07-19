# Energy ETL Pipeline

>Automated energy data pipeline — Extract, Transform, Load, orchestrated with Airflow.

## What it does?
This pipeline extracts energy usuage data from source, transforms it into star schema and loads it into sqlite3 - automated apache airflow

## Tech Stack
Python
Pandas
Sqlite
Apache Airflow
Star Schema(Data Warehouse)

## Pipeline
Extarct(CSV) >> Transform(Dim Table + Fact Table) >> Load(Sqlite)
