import os
import pandas as pd
import psycopg2

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

def compute_top_ctr(active_ads_views_path: str, output_folder: str) -> str:
    """Compute TopCTR model and write results to a temporary folder."""
    active_ads_views = pd.read_csv(active_ads_views_path)
    active_ads_views['is_click'] = (active_ads_views['type'] == 'click').astype(int)
    ctr = active_ads_views.groupby(['advertiser_id', 'product_id'])['is_click'].mean().reset_index()
    ctr.columns = ['advertiser_id', 'product_id', 'ctr']
    
    top_ctr = ctr.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'ctr')).reset_index(drop=True)
    output_path = os.path.join(output_folder, 'top_ctr.csv')
    top_ctr.to_csv(output_path, index=False)
    return output_path

def compute_top_product(active_product_views_path: str, output_folder: str) -> str:
    """Compute TopProduct model and write results to a temporary folder."""
    active_product_views = pd.read_csv(active_product_views_path)
    top_product = active_product_views.groupby(['advertiser_id', 'product_id']).size().reset_index(name='view_count')
    
    top_products = top_product.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'view_count')).reset_index(drop=True)
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
        
        # Create table for TopCTR if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_ctr (
            advertiser_id INT,
            product_id INT,
            ctr FLOAT,
            PRIMARY KEY (advertiser_id, product_id)
        );
        """)

        # Insert TopCTR results
        for _, row in top_ctr.iterrows():
            cursor.execute("""
            INSERT INTO top_ctr (advertiser_id, product_id, ctr)
            VALUES (%s, %s, %s)
            ON CONFLICT (advertiser_id, product_id) DO UPDATE
            SET ctr = EXCLUDED.ctr;
            """, (row['advertiser_id'], row['product_id'], row['ctr']))

        # Create table for TopProducts if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_product (
            advertiser_id INT,
            product_id INT,
            view_count INT,
            PRIMARY KEY (advertiser_id, product_id)
        );
        """)

        # Insert TopProducts results
        for _, row in top_product.iterrows():
            cursor.execute("""
            INSERT INTO top_product (advertiser_id, product_id, view_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (advertiser_id, product_id) DO UPDATE
            SET view_count = EXCLUDED.view_count;
            """, (row['advertiser_id'], row['product_id'], row['view_count']))

        # Commit changes
        connection.commit()
    except Exception as e:
        print("Error writing to database:", e)
    finally:
        if connection:
            cursor.close()
            connection.close()



if __name__ == '__main__':
    pass


