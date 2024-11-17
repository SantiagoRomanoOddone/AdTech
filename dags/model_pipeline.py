import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import sys
import os
# Adding the parent directory of 'scr' to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scr.logic import (
    filter_active_advertiser_views,
    filter_active_advertiser_products,
    compute_top_ctr,
    compute_top_product,
    write_to_db,
)

ads_views_path = 'data/ads_views.csv'
advertiser_path = 'data/advertiser_ids.csv'
product_views_path = 'data/product_views.csv'

# Define the DAG
with DAG(
    dag_id='models_pipeline',
    schedule_interval='@daily',  # Schedule to run daily
    start_date=datetime.datetime(2024, 11, 17),  # Start date of the DAG
    catchup=False,  # Do not backfill missing runs
) as dag:
    # Task 1: Filter active advertiser views
    filter_views_task = PythonOperator(
        task_id='filter_active_advertiser_views',
        python_callable=filter_active_advertiser_views,
        op_kwargs={
            'ads_views_path': ads_views_path,
            'advertiser_path': advertiser_path,
        },
    )

    # Task 2: Filter active advertiser products
    filter_products_task = PythonOperator(
        task_id='filter_active_advertiser_products',
        python_callable=filter_active_advertiser_products,
        op_kwargs={
            'product_views_path': product_views_path,
            'advertiser_path': advertiser_path,
        },
    )

    # Task 3: Compute TopCTR
    compute_top_ctr_task = PythonOperator(
        task_id='compute_top_ctr',
        python_callable=compute_top_ctr,
        op_kwargs={
            'active_ads_views': "{{ ti.xcom_pull(task_ids='filter_active_advertiser_views') }}",
        },
    )

    # Task 4: Compute TopProducts
    compute_top_product_task = PythonOperator(
        task_id='compute_top_products',
        python_callable=compute_top_product,
        op_kwargs={
            'active_product_views': "{{ ti.xcom_pull(task_ids='filter_active_advertiser_products') }}",
        },
    )

    # Task 5: Write results to the database
    write_to_db_task = PythonOperator(
        task_id='write_to_db',
        python_callable=write_to_db,
        op_kwargs={
            'top_ctr_path': "{{ ti.xcom_pull(task_ids='compute_top_ctr') }}",
            'top_product_path': "{{ ti.xcom_pull(task_ids='compute_top_products') }}",
        },
    )

    # Define dependencies
    filter_views_task >> compute_top_ctr_task
    filter_products_task >> compute_top_product_task
    [compute_top_ctr_task, compute_top_product_task] >> write_to_db_task



