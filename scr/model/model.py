import os
from datetime import datetime
import sys
import pandas as pd
import boto3
from dotenv import load_dotenv
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.utils import ensure_temp_folder_exists, read_file_from_s3, write_file_to_s3, write_to_db

    
def filter_active_advertiser_views(bucket_name: str, 
                                   s3_client,
                                   output_folder: str) -> str:
    """Filter view logs for active advertisers and write to a temporary folder."""
    ads_view = read_file_from_s3(bucket_name, 'ads_views.csv', s3_client)
    active_advertisers = read_file_from_s3(bucket_name, 'advertiser_ids.csv', s3_client)
    
    filtered_views = ads_view[ads_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]

    output_key = f"{output_folder}/active_ads_views.csv"
    write_file_to_s3(filtered_views, bucket_name, output_key, s3_client)
    return output_key 

def filter_active_advertiser_products(bucket_name: str, 
                                      s3_client,
                                      output_folder: str) -> str:
    """Filter product logs for active advertisers and write to a temporary folder."""
    product_view = read_file_from_s3(bucket_name, 'product_views.csv', s3_client)
    active_advertisers = read_file_from_s3(bucket_name, 'advertiser_ids.csv', s3_client)

    filtered_products = product_view[product_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]

    output_key = f"{output_folder}/active_product_views.csv"
    write_file_to_s3(filtered_products, bucket_name, output_key, s3_client)
    return output_key


def compute_top_ctr(bucket_name: str, 
                    s3_client,
                    output_folder: str,
                    active_ads_views_path: str, 
                    execution_date: datetime.date) -> str:
    """Compute TopCTR model and write results to a temporary folder."""
    # active_ads_views = pd.read_csv(active_ads_views_path)
    active_ads_views = read_file_from_s3(bucket_name, active_ads_views_path, s3_client)
    active_ads_views['is_click'] = (active_ads_views['type'] == 'click').astype(int)
    ctr = active_ads_views.groupby(['advertiser_id', 'product_id'])['is_click'].mean().reset_index()
    ctr.columns = ['advertiser_id', 'product_id', 'ctr']
    
    top_ctr = ctr.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'ctr')).reset_index(drop=True)
    top_ctr['date'] = execution_date
    top_ctr['model'] = 'top_ctr'

    output_key = f"{output_folder}/top_ctr.csv"
    write_file_to_s3(top_ctr, bucket_name, output_key, s3_client)
    return output_key

def compute_top_product(bucket_name: str, 
                        s3_client,
                        output_folder: str,
                        active_product_views_path: str, 
                        execution_date: datetime.date) -> str:
    """Compute TopProduct model and write results to a temporary folder."""

    active_product_views = read_file_from_s3(bucket_name, active_product_views_path, s3_client)
    top_product = active_product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='view_count')
    
    top_products = top_product.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'view_count')).reset_index(drop=True)
    top_products['date'] = execution_date
    top_products['model'] = 'top_product'

    output_key = f"{output_folder}/top_products.csv"
    write_file_to_s3(top_products, bucket_name, output_key, s3_client)
    return output_key


if __name__ == '__main__':

    # Load the appropriate .env file based on the environment
    load_dotenv()
    execution_date = datetime(2024,11,26)

    DB_CONFIG = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
    }
    BUCKET_NAME= os.getenv('BUCKET_NAME')
    s3_client = boto3.client(
        's3',
        region_name=os.getenv('REGION_NAME'),
        aws_access_key_id=os.getenv('ACCESS_KEY'),
        aws_secret_access_key=os.getenv('SECRET_KEY')
    )
    temp_folder = 'temp_data'

    # Ensure temp folder exists
    ensure_temp_folder_exists(temp_folder)
    
    # Test filter_active_advertiser_views
    active_ads_views_path = filter_active_advertiser_views(BUCKET_NAME, 
                                                           s3_client, 
                                                           temp_folder)
    print(f"Active advertiser views saved to: {active_ads_views_path}")
    
    # Test filter_active_advertiser_products
    active_product_views_path = filter_active_advertiser_products(BUCKET_NAME, 
                                                                  s3_client,
                                                                  temp_folder)
    print(f"Active advertiser products saved to: {active_product_views_path}")
    
    # Test compute_top_ctr
    top_ctr_path = compute_top_ctr(BUCKET_NAME, 
                                   s3_client, 
                                   temp_folder, 
                                   active_ads_views_path,
                                   execution_date)
    print(f"TopCTR results saved to: {top_ctr_path}")
    
    # Test compute_top_product
    top_product_path = compute_top_product(BUCKET_NAME,
                                           s3_client,
                                           temp_folder, 
                                           active_product_views_path,
                                           execution_date)
    print(f"TopProduct results saved to: {top_product_path}")

    write_to_db(BUCKET_NAME,
                s3_client,
                temp_folder, 
                top_ctr_path, 
                top_product_path, 
                DB_CONFIG)
    print("Results written to database.")


