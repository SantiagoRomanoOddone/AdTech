import os
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from dotenv import load_dotenv

def ensure_temp_folder_exists(folder_path: str):
    """Ensure that the temporary folder exists."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

def filter_active_advertiser_views(ads_views_path: str, advertiser_path: str, output_folder: str) -> str:
    """Filter view logs for active advertisers and write to a temporary folder."""
    ads_view = pd.read_csv(ads_views_path)
    active_advertisers = pd.read_csv(advertiser_path)
    
    filtered_views = ads_view[ads_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]
    output_path = os.path.join(output_folder, 'active_ads_views.csv')
    filtered_views.to_csv(output_path, index=False)
    return output_path

def filter_active_advertiser_products(product_views_path: str, advertiser_path: str, output_folder: str) -> str:
    """Filter product logs for active advertisers and write to a temporary folder."""
    product_view = pd.read_csv(product_views_path)
    active_advertisers = pd.read_csv(advertiser_path)
    
    filtered_products = product_view[product_view['advertiser_id'].isin(active_advertisers['advertiser_id'])]
    output_path = os.path.join(output_folder, 'active_product_views.csv')
    filtered_products.to_csv(output_path, index=False)
    return output_path

def compute_top_ctr(active_ads_views_path: str, output_folder: str, execution_date: datetime.date) -> str:
    """Compute TopCTR model and write results to a temporary folder."""
    active_ads_views = pd.read_csv(active_ads_views_path)
    active_ads_views['is_click'] = (active_ads_views['type'] == 'click').astype(int)
    ctr = active_ads_views.groupby(['advertiser_id', 'product_id'])['is_click'].mean().reset_index()
    ctr.columns = ['advertiser_id', 'product_id', 'ctr']
    
    top_ctr = ctr.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'ctr')).reset_index(drop=True)
    # top_ctr['date'] = datetime.today().date()
    top_ctr['date'] = execution_date.date()
    top_ctr['model'] = 'top_ctr'
    output_path = os.path.join(output_folder, 'top_ctr.csv')
    top_ctr.to_csv(output_path, index=False)
    return output_path

def compute_top_product(active_product_views_path: str, output_folder: str, execution_date: datetime.date) -> str:
    """Compute TopProduct model and write results to a temporary folder."""
    active_product_views = pd.read_csv(active_product_views_path)
    top_product = active_product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='view_count')
    
    top_products = top_product.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'view_count')).reset_index(drop=True)
    # top_products['date'] = datetime.today().date()
    top_products['date'] = execution_date
    top_products['model'] = 'top_product'
    output_path = os.path.join(output_folder, 'top_products.csv')
    top_products.to_csv(output_path, index=False)
    return output_path



def write_to_db(top_ctr_path: str, top_product_path: str, db_config: dict):
    """Write model results to PostgreSQL database."""
    # Load the results from CSV
    top_ctr = pd.read_csv(top_ctr_path)
    top_product = pd.read_csv(top_product_path)

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

if __name__ == '__main__':

    # Load the appropriate .env file based on the environment
    load_dotenv()
    execution_date = datetime(2024,11,26)

    DB_CONFIG = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT'))
    }
    
    # Paths
    ads_views_path = 'tmp/data/ads_views.csv'
    advertiser_path = 'tmp/data/advertiser_ids.csv'
    product_views_path = 'tmp/data/product_views.csv'
    temp_folder = 'tmp/temp_data'

    # Ensure temp folder exists
    ensure_temp_folder_exists(temp_folder)
    
    # Test filter_active_advertiser_views
    active_ads_views_path = filter_active_advertiser_views(ads_views_path, advertiser_path, temp_folder)
    print(f"Active advertiser views saved to: {active_ads_views_path}")
    
    # Test filter_active_advertiser_products
    active_product_views_path = filter_active_advertiser_products(product_views_path, advertiser_path, temp_folder)
    print(f"Active advertiser products saved to: {active_product_views_path}")
    
    # Test compute_top_ctr
    top_ctr_path = compute_top_ctr(active_ads_views_path, temp_folder, execution_date)
    print(f"TopCTR results saved to: {top_ctr_path}")
    
    # Test compute_top_product
    top_product_path = compute_top_product(active_product_views_path, temp_folder, execution_date)
    print(f"TopProduct results saved to: {top_product_path}")

    write_to_db(top_ctr_path, top_product_path, DB_CONFIG)
    print("Results written to database.")


