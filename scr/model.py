import os
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import boto3
from io import StringIO
from dotenv import load_dotenv

def ensure_temp_folder_exists(folder_path: str):
    """Ensure that the temporary folder exists."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
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

def write_to_db(bucket_name: str, 
                s3_client,
                output_folder: str,
                top_ctr_path: str, 
                top_product_path: str, 
                db_config: dict):
    """Write model results to PostgreSQL database."""
    # Load the results
    top_ctr = read_file_from_s3(bucket_name, top_ctr_path, s3_client)
    top_product = read_file_from_s3(bucket_name, top_product_path, s3_client)
    
    # Connect to PostgreSQL
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        # Create table for recommendations if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS recommendations (
            advertiser_id VARCHAR(20),
            product_id VARCHAR(20),
            metric FLOAT,
            date DATE,
            model VARCHAR(20),
            PRIMARY KEY (advertiser_id, product_id, model, date)
        );
        """)

       # Insert TopCTR results
        insert_recommendations(cursor, top_ctr, 'ctr')

        # Insert TopProducts results
        insert_recommendations(cursor, top_product, 'view_count')

        # Commit changes
        connection.commit()
    except Exception as e:
        print("Error writing to database:", e)
    finally:
        if connection:
            cursor.close()
            connection.close()

def insert_recommendations(cursor, data, metric_column, date_format='%Y-%m-%d'):
    for _, row in data.iterrows():
        try:
            advertiser_id = str(row['advertiser_id'])
            product_id = str(row['product_id'])
            metric = round(float(row[metric_column]), 5)
            date = datetime.strptime(row['date'], date_format)  # Adjust the date format as needed
            model = row['model']
            cursor.execute("""
            INSERT INTO recommendations (advertiser_id, product_id, metric, date, model)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (advertiser_id, product_id, model, date)
            DO UPDATE SET metric = EXCLUDED.metric, date = EXCLUDED.date;
            """, (advertiser_id, product_id, metric, date, model))
        except ValueError as e:
            print(f"Skipping row due to error: {e}")




def read_file_from_s3(bucket_name: str, file_key: str, s3_client) -> pd.DataFrame:
    """
    Read a CSV file from an S3 bucket and return a pandas DataFrame.
    """
    file_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = file_object['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content))
    return df

def write_file_to_s3(df: pd.DataFrame, bucket_name: str, file_key: str, s3_client):
    """Write a DataFrame to a CSV file in S3."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())


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

    # Files
    # ads_views_input = 'ads_views.csv'
    # advertiser_input = 'advertiser_ids.csv'
    # product_views_input = 'product_views.csv'
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


