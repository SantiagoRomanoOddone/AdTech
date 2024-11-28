import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import boto3
from dotenv import load_dotenv

# Adding the parent directory of 'scr' to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scr.utils.utils import write_to_db
from scr.model.model import (
    filter_active_advertiser_views,
    filter_active_advertiser_products,
    compute_top_ctr,
    compute_top_product
)
load_dotenv()
# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
}

# S3 Configuration
BUCKET_NAME = os.getenv('BUCKET_NAME')
s3_client = boto3.client(
    's3',
    region_name=os.getenv('REGION_NAME'),
    aws_access_key_id=os.getenv('ACCESS_KEY'),
    aws_secret_access_key=os.getenv('SECRET_KEY')
    )

# Define the DAG
with DAG(
    dag_id='models_pipeline',
    schedule_interval='@daily',  # Schedule to run daily
    start_date=datetime.datetime(2024, 11, 20),  # Start date of the DAG
    catchup=True,  # False: Do not backfill missing runs
    max_active_runs=1,
    ) as dag:
    # Task 1: Filter active advertiser views
    filter_views_task = PythonOperator(
        task_id='filter_active_advertiser_views',
        python_callable=filter_active_advertiser_views,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            's3_client': s3_client,  
            'output_folder': 'temp_data',
        },
    )

    # Task 2: Filter active advertiser products
    filter_products_task = PythonOperator(
        task_id='filter_active_advertiser_products',
        python_callable=filter_active_advertiser_products,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            's3_client': s3_client,
            'output_folder': 'temp_data',
        },
    )

    # Task 3: Compute TopCTR
    compute_top_ctr_task = PythonOperator(
        task_id='compute_top_ctr',
        python_callable=compute_top_ctr,
        provide_context=True,  # Pass execution_date to the callable
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            's3_client': s3_client,
            'output_folder': 'temp_data',
            'active_ads_views_path': "{{ ti.xcom_pull(task_ids='filter_active_advertiser_views') }}",
            'execution_date': "{{ execution_date }}",
        },
    )

    # Task 4: Compute TopProducts
    compute_top_product_task = PythonOperator(
        task_id='compute_top_product',
        python_callable=compute_top_product,
        provide_context=True,  # Pass execution_date to the callable
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            's3_client': s3_client,
            'output_folder': 'temp_data',
            'active_product_views_path': "{{ ti.xcom_pull(task_ids='filter_active_advertiser_products') }}",
            'execution_date': "{{ execution_date }}",
        },
    )

    # Task 5: Write results to the database
    write_to_db_task = PythonOperator(
        task_id='write_to_db',
        python_callable=write_to_db,
        op_kwargs={
            'bucket_name': BUCKET_NAME,
            's3_client': s3_client,
            'output_folder': 'temp_data',
            'top_ctr_path': "{{ ti.xcom_pull(task_ids='compute_top_ctr') }}",
            'top_product_path': "{{ ti.xcom_pull(task_ids='compute_top_product') }}",
            'db_config': DB_CONFIG,
        },
    )

    # Task Dependencies
    filter_views_task >> compute_top_ctr_task
    filter_products_task >> compute_top_product_task
    [compute_top_ctr_task, compute_top_product_task] >> write_to_db_task