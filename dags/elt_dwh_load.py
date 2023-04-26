from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lesson_data_vault',
    default_args=default_args,
    description='Load lesson data into Data Vault and create a data mart',
    schedule_interval=timedelta(days=1),
    catchup=False
)

source_conn = 'source'
dwh_conn = 'dwh'

# Load hubs into Data Vault tables
load_hubs = PostgresOperator(
    task_id='load_hubs',
    sql='./models/data_vault/load_hubs.sql',
    postgres_conn_id=dwh_conn,
    dag=dag,
)

# Load links into Data Vault tables
load_links = PostgresOperator(
    task_id='load_links',
    sql='./models/data_vault/load_links.sql',
    postgres_conn_id=dwh_conn,
    dag=dag,
)

# Load satelits into Data Vault tables
load_satelits = PostgresOperator(
    task_id='load_satelits',
    sql='./models/data_vault/load_satelits.sql',
    postgres_conn_id=dwh_conn,
    dag=dag,
)

# Update last processed time
last_processed_timestamp_update = PostgresOperator(
    task_id='last_processed_timestamp_update',
    sql='./models/data_vault/last_processed_timestamp.sql',
    postgres_conn_id=dwh_conn,
    dag=dag,
)


# Create a data mart from Data Vault
create_data_mart = PostgresOperator(
    task_id='load_lesson_data_mart',
    sql='models/mart/lessons.sql',
    postgres_conn_id=dwh_conn,
    dag=dag,
)

load_hubs >> [load_links, load_satelits] >> [ last_processed_timestamp_update,create_data_mart]
