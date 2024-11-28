
import os
import pandas as pd
from io import StringIO
from dateutil.parser import parse
import psycopg2


def ensure_temp_folder_exists(folder_path: str):
    """Ensure that the temporary folder exists."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


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

def insert_recommendations(cursor, data, metric_column):
    for _, row in data.iterrows():
        try:
            advertiser_id = str(row['advertiser_id'])
            product_id = str(row['product_id'])
            metric = round(float(row[metric_column]), 5)
            date = parse(row['date']).date()  # Adjust the date format as needed
            model = row['model']
            cursor.execute("""
            INSERT INTO recommendations (advertiser_id, product_id, metric, date, model)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (advertiser_id, product_id, model, date)
            DO UPDATE SET metric = EXCLUDED.metric, date = EXCLUDED.date;
            """, (advertiser_id, product_id, metric, date, model))
        except ValueError as e:
            print(f"Skipping row due to error: {e}")
