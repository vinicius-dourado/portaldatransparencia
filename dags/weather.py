# Importing Airflow libraries
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.postgres_operator import PostgresOperator

# Importing Python Libraries
from datetime import datetime, timedelta
import time
import json
import os
import pandas as pd
from pandas import json_normalize
import csv, sqlite3
import glob
import requests
from geopy.geocoders import Nominatim
from pyarrow import json as pjson
import pyarrow.parquet as pq

#
from scripts_python.deduplicate import execute as dedup
from scripts_python.getdata import api as getdata
import scripts_python.location



# Location used to copy files into Postgres
def get_files_current():
     for i in glob.glob('data/weather/staging/current_*.csv'):
         return i.replace('data/weather/staging/','/opt/airflow/data/weather/staging/')

def get_file_hourly():
    for i in glob.glob(f'data/weather/staging/hourly_*.csv'):
        return i.replace('data/weather/staging/','/opt/airflow/data/weather/staging/')

with DAG(
    dag_id='weather',
    start_date=datetime(2022, 8, 10),
    schedule_interval=None
) as dag:

    create_raw= BashOperator(
        task_id='clean_raw',
        bash_command="rm -rf '/opt/airflow/data/weather/raw/'; mkdir -p '/opt/airflow/data/weather/raw/'"
    )    

    create_staging= BashOperator(
        task_id='clean_staging',
        bash_command="rm -rf '/opt/airflow/data/weather/staging/'; mkdir -p '/opt/airflow/data/weather/staging/'"
    )  

    get_api = PythonOperator(
        task_id='get_api',
        python_callable=getdata,
        trigger_rule='all_success'
    )

    deduplicate_rows = PythonOperator(
        task_id='deduplicate_rows',
        python_callable=dedup,
        trigger_rule='all_success'
    )
  
    with TaskGroup('postgres_tasks') as postgres_tasks:
        
    # Create table Hourly
        create_hourly_table = PostgresOperator(
            task_id='create_hourly_table',
            postgres_conn_id='my_postgres',
            sql='''             
                CREATE TABLE IF NOT EXISTS hourly (
                    DT VARCHAR(50),
                    TEMP VARCHAR(50),
                    FELLS_LIKE VARCHAR(50),
                    PRESSURE VARCHAR(50),
                    HUMIDITY VARCHAR(50),
                    DEW_POINT VARCHAR(50),
                    UVI VARCHAR(50),
                    CLOUDS VARCHAR(50),
                    VISIBILITY VARCHAR(20),
                    WIND_SPEED VARCHAR(20),
                    WIND_DEG VARCHAR(50),
                    CITY VARCHAR(150) NULL,
                    STATE VARCHAR(150) NULL,
                    COUNTRY VARCHAR(150) NULL                                    
                );                
                '''
        )
    # Create table Hourly
        create_current_table = PostgresOperator(
            task_id='create_current_table',
            postgres_conn_id='my_postgres',
            sql='''             
                CREATE TABLE IF NOT EXISTS current (
                    DT VARCHAR(50),
                    TEMP VARCHAR(50),
                    FELLS_LIKE VARCHAR(50),
                    PRESSURE VARCHAR(50),
                    HUMIDITY VARCHAR(50),
                    DEW_POINT VARCHAR(50),
                    UVI VARCHAR(50),
                    CLOUDS VARCHAR(50),
                    VISIBILITY VARCHAR(20),
                    WIND_SPEED VARCHAR(20),
                    WIND_DEG VARCHAR(50),
                    CITY VARCHAR(150) NULL,
                    STATE VARCHAR(150) NULL,
                    COUNTRY VARCHAR(150) NULL                                        
                );                
                '''
        )

    with TaskGroup('truncate_postgres_tasks') as truncate_postgres_tasks:
        
     # TRUNCATE table CURRENT
        truncate_hourly_table = PostgresOperator(
            task_id='truncate_hourly_table',
            postgres_conn_id='my_postgres',
            sql='''             
                TRUNCATE TABLE hourly;                
                '''
        )
    # TRUNCATE table CURRENT
        truncate_current_table = PostgresOperator(
            task_id='truncate_current_table',
            postgres_conn_id='my_postgres',
            sql='''             
                TRUNCATE TABLE current;                
                '''
        )

    with TaskGroup('insert_postgres_tasks') as insert_postgres_tasks:
        
     # insert into table HOURLY
        insert_hourly_table = PostgresOperator(
            task_id='insert_hourly_table',
            postgres_conn_id='my_postgres',
            sql=f'''
                    COPY hourly
                    FROM '%s'
                    DELIMITER ','
                    csv header
                    ;
                ''' % get_file_hourly()
        )
    # insert into table CURRENT
        insert_current_table = PostgresOperator(
            task_id='insert_current_table',
            postgres_conn_id='my_postgres',
            sql=f'''
                    COPY current
                    FROM '%s'
                    DELIMITER ','
                    csv header
                    ;               
                '''% get_files_current()
        )       

    dbt_tests= BashOperator(
        task_id='dbt_tests',
        bash_command="dbt test --project-dir /opt/airflow/dbt/airflow_dbt"
    )    

    dbt_model= BashOperator(
        task_id='dbt_model',
        bash_command="dbt run --project-dir /opt/airflow/dbt/airflow_dbt"
    )    

create_raw >> create_staging >> get_api >> deduplicate_rows >> postgres_tasks >> truncate_postgres_tasks >> insert_postgres_tasks >> dbt_tests >> dbt_model
