import json
from airflow.models.connection import Connection


c = Connection(
    conn_id='api',
    conn_type='HTTP',
    description=None,
    login=None,
    password='f03d3da3e4aa605d6d5c5a01a13ca69f',
    host='http://api.openweathermap.org/data/2.5/onecall/timemachine',
    port=None,
    schema=None,
    extra=None
)

c = Connection(
    conn_id='my_postgres',
    conn_type='postgres',
    description=None,
    login='airflow',
    password='airflow',
    host='postgres',
    port='5432',
    schema='airflow',
    extra=None
)

print(f"AIRFLOW_CONN_{c.conn_id.upper()}='{c.get_uri()}'")
